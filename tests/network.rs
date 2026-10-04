use aura_osm::network;
use reqwest::{Client, redirect::Policy};
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::thread::{self, JoinHandle};
use std::time::Duration;

fn response(bytes: &'static [u8], requests: usize) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let worker = thread::spawn(move || {
        for _ in 0..requests {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut reader = BufReader::new(&stream);
            loop {
                let mut line = String::new();
                assert_ne!(reader.read_line(&mut line).unwrap(), 0);
                if line == "\r\n" {
                    break;
                }
            }
            stream.write_all(bytes).unwrap();
        }
    });
    (url, worker)
}

#[test]
fn retry_backoff_caps_at_one_day_including_overflow_and_server_hints() {
    for (initial, attempt, expected) in [
        (Duration::from_secs(60), 0, Duration::from_secs(60)),
        (Duration::from_secs(60), 1, Duration::from_secs(120)),
        (Duration::from_secs(60), 10, Duration::from_secs(61_440)),
        (Duration::from_secs(60), 11, Duration::from_secs(86_400)),
        (
            Duration::from_nanos(1),
            u32::MAX,
            Duration::from_secs(86_400),
        ),
        (Duration::MAX, 0, Duration::from_secs(86_400)),
        (Duration::ZERO, u32::MAX, Duration::ZERO),
    ] {
        assert_eq!(network::backoff(initial, attempt), expected);
    }
    for (hint, expected) in [(90, 90), (30, 60), (172_800, 86_400)] {
        let error = anyhow::Error::new(network::Transient("source unavailable".into()))
            .context(network::RetryAfter(Duration::from_secs(hint)));
        let error = network::summary(&error, "region download failed".into());
        assert!(network::retryable(&error));
        assert_eq!(
            network::retry_delay(&error, Duration::from_secs(60), 0),
            Duration::from_secs(expected)
        );
    }
}

#[test]
fn persistent_retry_recovers_after_repeated_network_failures_and_stops_on_local_errors() {
    let mut calls = 0;
    let value = network::retry(Duration::ZERO, || {
        calls += 1;
        if calls <= 32 {
            return Err(network::Transient("connection failed".into()).into());
        }
        Ok(42)
    })
    .unwrap();
    assert_eq!(calls, 33);
    assert_eq!(value, 42);

    let mut calls = 0;
    let result: anyhow::Result<()> = network::retry(Duration::ZERO, || {
        calls += 1;
        anyhow::bail!("invalid data")
    });
    assert!(result.unwrap_err().to_string().contains("invalid data"));
    assert_eq!(calls, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn broken_response_framing_is_retryable() {
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    for bytes in [
        &b"HTTP/1.1 200 OK\r\nContent-Length: 20\r\nConnection: close\r\n\r\ndata"[..],
        &b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n4\r\ndata\r\ninvalid\r\n"[..],
    ] {
        let (url, worker) = response(bytes, 1);
        let mut response = client.get(url).send().await.unwrap();
        let error = loop {
            match response.chunk().await {
                Ok(Some(_)) => {}
                Ok(None) => panic!("Broken response framing was accepted"),
                Err(error) => break error,
            }
        };
        worker.join().unwrap();
        assert!(error.is_decode(), "{error:?}");
        let error = network::request(error);
        assert!(network::retryable(&error), "{error:#}");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_request_is_not_retryable() {
    let error = Client::new().get("invalid URL").send().await.unwrap_err();
    assert!(error.is_builder());
    assert!(!network::retryable(&network::request(error)));
}

#[tokio::test(flavor = "current_thread")]
async fn rejected_redirect_is_not_retryable() {
    let (url, worker) = response(b"HTTP/1.1 302 Found\r\nLocation: http://unsafe.example/\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", 1);
    let error = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .redirect(Policy::custom(|attempt| attempt.error("Unsafe redirect")))
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap_err();
    worker.join().unwrap();
    assert!(error.is_redirect());
    assert!(!network::retryable(&network::request(error)));
}

#[tokio::test(flavor = "current_thread")]
async fn redirect_loop_marked_transient_is_retryable() {
    let (url, worker) = response(b"HTTP/1.1 301 Moved Permanently\r\nLocation: /\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", 3);
    let error = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .redirect(Policy::custom(|attempt| {
            if attempt.previous().len() >= 3 {
                attempt.error(network::Transient("Download redirect limit reached".into()))
            } else {
                attempt.follow()
            }
        }))
        .build()
        .unwrap()
        .get(url)
        .send()
        .await
        .unwrap_err();
    worker.join().unwrap();
    assert!(error.is_redirect());
    let error = network::request(error);
    assert!(network::retryable(&error), "{error:#}");
    assert_eq!(error.to_string(), "Download redirect limit reached");
}
