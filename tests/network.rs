use aura_osm::network;
use reqwest::{Client, redirect::Policy};
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::thread::{self, JoinHandle};
use std::time::Duration;

fn response(bytes: &'static [u8]) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let worker = thread::spawn(move || {
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
    });
    (url, worker)
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
        let (url, worker) = response(bytes);
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
    let (url, worker) = response(b"HTTP/1.1 302 Found\r\nLocation: http://unsafe.example/\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
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
