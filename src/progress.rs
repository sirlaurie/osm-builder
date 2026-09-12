use std::io::{IsTerminal, Write};
use std::sync::{Arc, LazyLock, Mutex};

static OUTPUT: LazyLock<Arc<Mutex<Output>>> = LazyLock::new(|| {
    Arc::new(Mutex::new(Output {
        writer: Box::new(std::io::stderr()),
        terminal: std::io::stderr().is_terminal(),
        columns: None,
        rows: 0,
    }))
});

pub(crate) struct ProgressLine {
    output: Arc<Mutex<Output>>,
}

struct Output {
    writer: Box<dyn Write + Send>,
    terminal: bool,
    columns: Option<usize>,
    rows: usize,
}

impl Default for ProgressLine {
    fn default() -> Self {
        Self {
            output: Arc::clone(&OUTPUT),
        }
    }
}

impl ProgressLine {
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
            })),
        }
    }

    pub(crate) fn is_terminal(&self) -> bool {
        self.output.lock().expect("progress output lock").terminal
    }

    pub(crate) fn render(&mut self, line: &str) {
        self.output
            .lock()
            .expect("progress output lock")
            .render(line);
    }
}

impl Drop for ProgressLine {
    fn drop(&mut self) {
        let mut output = self.output.lock().expect("progress output lock");
        if output.terminal && output.rows > 0 {
            let _ = writeln!(output.writer);
            let _ = output.writer.flush();
            output.rows = 0;
        }
    }
}

pub(crate) fn message(line: &str) {
    OUTPUT.lock().expect("progress output lock").message(line);
}

impl Output {
    fn clear(&mut self) {
        if self.rows > 1 {
            let _ = write!(self.writer, "\x1b[{}A", self.rows - 1);
        }
        let _ = write!(self.writer, "\r\x1b[0J");
        self.rows = 0;
    }

    fn render(&mut self, line: &str) {
        if self.terminal {
            self.clear();
            let _ = write!(self.writer, "{line}");
            let _ = self.writer.flush();
            let columns = self
                .columns
                .or_else(|| {
                    terminal_size::terminal_size_of(std::io::stderr())
                        .map(|(width, _)| usize::from(width.0))
                })
                .filter(|columns| *columns > 0)
                .unwrap_or(80);
            self.rows = line.len().div_ceil(columns).max(1);
        } else {
            let _ = writeln!(self.writer, "{line}");
        }
    }

    fn message(&mut self, line: &str) {
        if self.terminal && self.rows > 0 {
            self.clear();
        }
        let _ = writeln!(self.writer, "{line}");
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
                    "\r\x1b[0J12345678\x1b[1A\r\x1b[0J12\r\x1b[0Jdownload retry\n\r\x1b[0J34\n"
                } else {
                    "12345678\n12\ndownload retry\n34\n"
                }
            );
        }
    }
}
