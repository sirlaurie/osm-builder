use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io::{IsTerminal, Write};
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::{Arc, LazyLock, Mutex};

thread_local! {
    static REGION: RefCell<Option<String>> = const { RefCell::new(None) };
}

pub(crate) fn region(name: &str) -> impl Drop {
    struct Scope(Option<String>, PhantomData<Rc<()>>);
    impl Drop for Scope {
        fn drop(&mut self) {
            REGION.with(|region| *region.borrow_mut() = self.0.take());
        }
    }
    Scope(
        REGION.with(|region| region.replace(Some(name.to_owned()))),
        PhantomData,
    )
}

static OUTPUT: LazyLock<Arc<Mutex<Output>>> = LazyLock::new(|| {
    Arc::new(Mutex::new(Output {
        writer: Box::new(std::io::stderr()),
        terminal: std::io::stderr().is_terminal(),
        columns: None,
        rows: 0,
        next_id: 0,
        lines: BTreeMap::new(),
    }))
});

pub(crate) struct ProgressLine {
    output: Arc<Mutex<Output>>,
    id: u64,
    label: Option<String>,
}

struct Output {
    writer: Box<dyn Write + Send>,
    terminal: bool,
    columns: Option<usize>,
    rows: usize,
    next_id: u64,
    lines: BTreeMap<u64, String>,
}

impl Default for ProgressLine {
    fn default() -> Self {
        Self::shared(
            Arc::clone(&OUTPUT),
            REGION.with(|region| region.borrow().clone()),
        )
    }
}

impl ProgressLine {
    fn shared(output: Arc<Mutex<Output>>, label: Option<String>) -> Self {
        let id = {
            let mut output = output.lock().expect("progress output lock");
            let id = output.next_id;
            output.next_id += 1;
            id
        };
        Self { output, id, label }
    }

    pub(crate) fn for_region(region: &str) -> Self {
        Self::shared(Arc::clone(&OUTPUT), Some(region.to_owned()))
    }

    #[cfg(test)]
    pub(crate) fn with_output(
        writer: Box<dyn Write + Send>,
        terminal: bool,
        columns: Option<usize>,
    ) -> Self {
        Self {
            output: Arc::new(Mutex::new(Output {
                writer,
                terminal,
                columns,
                rows: 0,
                next_id: 1,
                lines: BTreeMap::new(),
            })),
            id: 0,
            label: None,
        }
    }

    pub(crate) fn is_terminal(&self) -> bool {
        self.output.lock().expect("progress output lock").terminal
    }

    pub(crate) fn render(&mut self, line: &str) {
        let labelled = self
            .label
            .as_ref()
            .map(|label| format!("[{label}] {}", line.strip_prefix("[osm] ").unwrap_or(line)));
        self.output
            .lock()
            .expect("progress output lock")
            .render(self.id, labelled.as_deref().unwrap_or(line));
    }
}

impl Drop for ProgressLine {
    fn drop(&mut self) {
        let mut output = self.output.lock().expect("progress output lock");
        if let Some(line) = output.lines.remove(&self.id) {
            output.clear();
            let _ = writeln!(output.writer, "{line}");
            output.paint();
        }
    }
}

pub(crate) fn message(line: &str) {
    REGION.with(|region| {
        let region = region.borrow();
        let labelled = region
            .as_ref()
            .filter(|_| !line.starts_with('['))
            .map(|region| format!("[{region}] {line}"));
        OUTPUT
            .lock()
            .expect("progress output lock")
            .message(labelled.as_deref().unwrap_or(line));
    });
}

impl Output {
    fn clear(&mut self) {
        if self.rows > 1 {
            let _ = write!(self.writer, "\x1b[{}A", self.rows - 1);
        }
        let _ = write!(self.writer, "\r\x1b[0J");
        self.rows = 0;
    }

    fn render(&mut self, id: u64, line: &str) {
        if self.terminal {
            self.lines.insert(id, line.to_owned());
            self.clear();
            self.paint();
        } else {
            let _ = writeln!(self.writer, "{line}");
        }
    }

    fn paint(&mut self) {
        if !self.lines.is_empty() {
            let line = self.lines.values().cloned().collect::<Vec<_>>().join("\n");
            let _ = write!(self.writer, "{line}");
            let columns = self
                .columns
                .or_else(|| {
                    terminal_size::terminal_size_of(std::io::stderr())
                        .map(|(width, _)| usize::from(width.0))
                })
                .filter(|columns| *columns > 0)
                .unwrap_or(80);
            self.rows = self
                .lines
                .values()
                .map(|line| line.len().div_ceil(columns).max(1))
                .sum();
        }
        let _ = self.writer.flush();
    }

    fn message(&mut self, line: &str) {
        if self.terminal && self.rows > 0 {
            self.clear();
        }
        let _ = writeln!(self.writer, "{line}");
        self.paint();
        let _ = self.writer.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);

    impl Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn shared_progress_preserves_messages_and_finishes_terminal_lines() {
        for terminal in [false, true] {
            let buffer = Buffer(Arc::default());
            let mut progress =
                ProgressLine::with_output(Box::new(buffer.clone()), terminal, Some(4));
            progress.render("12345678");
            progress.render("12");
            progress.output.lock().unwrap().message("download retry");
            progress.render("34");
            drop(progress);
            let text = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
            assert_eq!(
                text,
                if terminal {
                    "\r\x1b[0J12345678\x1b[1A\r\x1b[0J12\r\x1b[0Jdownload retry\n12\r\x1b[0J34\r\x1b[0J34\n"
                } else {
                    "12345678\n12\ndownload retry\n34\n"
                }
            );
        }
    }

    #[test]
    fn concurrent_regions_keep_independent_lines_and_finish_in_completion_order() {
        let buffer = Buffer(Arc::default());
        let mut first = ProgressLine::with_output(Box::new(buffer.clone()), true, Some(80));
        first.label = Some("a".into());
        let output = Arc::clone(&first.output);
        let mut second = ProgressLine::shared(Arc::clone(&output), Some("b".into()));
        first.render("[osm] upload 1");
        second.render("[osm] build 1");
        first.render("upload 2");
        assert_eq!(
            output
                .lock()
                .unwrap()
                .lines
                .values()
                .cloned()
                .collect::<Vec<_>>(),
            ["[a] upload 2", "[b] build 1"]
        );
        drop(first);
        output.lock().unwrap().message("finished a");
        second.render("build 2");
        drop(second);
        let output = output.lock().unwrap();
        assert!(output.lines.is_empty());
        assert_eq!(output.rows, 0);
        let text = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
        assert!(text.contains("[a] upload 2\n[b] build 1"));
        assert!(text.ends_with("[b] build 2\r\x1b[0J[b] build 2\n"));
    }
}
