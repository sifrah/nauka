//! Connection deadlines shared by the native HTTP and HTTPS doors.
//!
//! Hyper's header timer also expires idle HTTP/1 keep-alive connections,
//! but only after protocol detection. Bound that initial detection too:
//! otherwise a socket sending nothing (or a partial HTTP/2 preface) never
//! reaches Hyper's timer. Neither deadline runs during a request body,
//! handler, or response body: slow uploads and paced downloads are valid.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use axum_server::accept::Accept;
use hyper_util::rt::TokioTimer;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::Sleep;

pub(crate) const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(30);
const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

pub(crate) fn with_timeouts<A>(
    server: axum_server::Server<SocketAddr, A>,
) -> axum_server::Server<SocketAddr, ProtocolAcceptor<A>> {
    with_timeout(server, HEADER_READ_TIMEOUT)
}

fn with_timeout<A>(
    mut server: axum_server::Server<SocketAddr, A>,
    timeout: Duration,
) -> axum_server::Server<SocketAddr, ProtocolAcceptor<A>> {
    server
        .http_builder()
        .http1()
        .timer(TokioTimer::new())
        .header_read_timeout(timeout);
    // Once a complete HTTP/2 preface has been received, a silent peer
    // must still answer protocol pings. Responsive HTTP/2 connections
    // may remain idle; active streams have no application deadline.
    server
        .http_builder()
        .http2()
        .timer(TokioTimer::new())
        .keep_alive_interval(timeout)
        .keep_alive_timeout(timeout);
    server.map(|inner| ProtocolAcceptor { inner, timeout })
}

/// Wrap the accepted plaintext stream, including after a TLS handshake.
/// Rustls keeps its own handshake deadline and certificate reload support.
#[derive(Clone)]
pub(crate) struct ProtocolAcceptor<A> {
    inner: A,
    timeout: Duration,
}

impl<I, S, A> Accept<I, S> for ProtocolAcceptor<A>
where
    A: Accept<I, S>,
    A::Future: Send + 'static,
    A::Stream: Send + 'static,
    A::Service: Send + 'static,
{
    type Stream = ProtocolIo<A::Stream>;
    type Service = A::Service;
    type Future = Pin<Box<dyn Future<Output = io::Result<(Self::Stream, Self::Service)>> + Send>>;

    fn accept(&self, stream: I, service: S) -> Self::Future {
        let future = self.inner.accept(stream, service);
        let timeout = self.timeout;
        Box::pin(async move {
            let (stream, service) = future.await?;
            Ok((ProtocolIo::new(stream, timeout), service))
        })
    }
}

pub(crate) struct ProtocolIo<I> {
    inner: I,
    matched: usize,
    deadline: Option<Pin<Box<Sleep>>>,
}

impl<I> ProtocolIo<I> {
    fn new(inner: I, timeout: Duration) -> Self {
        Self {
            inner,
            matched: 0,
            deadline: Some(Box::pin(tokio::time::sleep(timeout))),
        }
    }
}

impl<I: AsyncRead + Unpin> AsyncRead for ProtocolIo<I> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        // A single deadline for the whole preface, never extended by a
        // trickle of bytes. Stop polling it permanently once Hyper can
        // choose a protocol, before any application work can start.
        if let Some(deadline) = &mut self.deadline {
            if deadline.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "HTTP protocol detection timed out",
                )));
            }
        }
        let before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                if self.deadline.is_some() {
                    let bytes = &buf.filled()[before..];
                    if bytes.is_empty() {
                        self.deadline = None; // EOF
                    } else {
                        for &byte in bytes {
                            if byte != H2_PREFACE[self.matched] {
                                self.deadline = None;
                                break;
                            }
                            self.matched += 1;
                            if self.matched == H2_PREFACE.len() {
                                self.deadline = None;
                                break;
                            }
                        }
                    }
                }
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl<I: AsyncWrite + Unpin> AsyncWrite for ProtocolIo<I> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use axum::body::{to_bytes, Body};
    use axum::extract::{ConnectInfo, Request};
    use axum::http::{header, Response};
    use axum::routing::{get, post};
    use axum::Router;
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::{sleep, timeout};

    const LIMIT: Duration = Duration::from_millis(400);
    const SLOW: Duration = Duration::from_millis(1200);
    const TEST_BOUND: Duration = Duration::from_secs(5);

    struct RunningServer {
        addr: SocketAddr,
        handle: axum_server::Handle<SocketAddr>,
    }

    impl Drop for RunningServer {
        fn drop(&mut self) {
            self.handle.shutdown();
        }
    }

    fn app() -> Router {
        Router::new()
            .route("/", get(|| async { "ok" }))
            .route(
                "/peer",
                get(|ConnectInfo(peer): ConnectInfo<SocketAddr>| async move { peer.to_string() }),
            )
            .route(
                "/slow",
                get(|| async {
                    sleep(SLOW).await;
                    "slow"
                }),
            )
            .route(
                "/upload",
                post(|request: Request| async move {
                    to_bytes(request.into_body(), 1024).await.unwrap()
                }),
            )
            .route(
                "/stream",
                get(|| async {
                    let (tx, rx) = tokio::sync::mpsc::channel(1);
                    tokio::spawn(async move {
                        tx.send(Ok::<_, io::Error>(bytes::Bytes::from_static(b"abc")))
                            .await
                            .unwrap();
                        // Deliberately no bytes on the wire for longer
                        // than the header/idle deadline, as with pacing.
                        sleep(SLOW).await;
                        let _ = tx.send(Ok(bytes::Bytes::from_static(b"def"))).await;
                    });
                    Response::builder()
                        .header(header::CONTENT_LENGTH, "6")
                        .body(Body::from_stream(
                            tokio_stream::wrappers::ReceiverStream::new(rx),
                        ))
                        .unwrap()
                }),
            )
    }

    async fn start_http() -> RunningServer {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = axum_server::Handle::new();
        let server = with_timeout(axum_server::Server::from_listener(listener), LIMIT)
            .handle(handle.clone());
        tokio::spawn(async move {
            server
                .serve(app().into_make_service_with_connect_info::<SocketAddr>())
                .await
                .unwrap();
        });
        RunningServer { addr, handle }
    }

    async fn request(io: &mut (impl AsyncWrite + Unpin), path: &str) {
        io.write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
            .await
            .unwrap();
        io.flush().await.unwrap();
    }

    async fn response(io: &mut (impl AsyncRead + Unpin)) -> Vec<u8> {
        timeout(TEST_BOUND, async {
            let mut headers = Vec::new();
            while !headers.ends_with(b"\r\n\r\n") {
                headers.push(io.read_u8().await.unwrap());
                assert!(headers.len() < 8192);
            }
            let headers = String::from_utf8(headers).unwrap();
            assert!(headers.starts_with("HTTP/1.1 200"), "{headers}");
            let length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap();
            let mut body = vec![0; length];
            io.read_exact(&mut body).await.unwrap();
            body
        })
        .await
        .expect("response must complete")
    }

    async fn closed(io: &mut (impl AsyncRead + Unpin)) {
        let mut remaining = Vec::new();
        match timeout(TEST_BOUND, io.read_to_end(&mut remaining)).await {
            Ok(Ok(_)) => {}
            // TLS may end without close_notify when the HTTP connection
            // is dropped; reset is also a valid TCP close on unread data.
            Ok(Err(e))
                if matches!(
                    e.kind(),
                    io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset
                ) => {}
            other => panic!("connection did not close within the bound: {other:?}"),
        }
    }

    #[tokio::test]
    async fn silent_tcp_connections_expire_before_protocol_detection() {
        let server = start_http().await;
        let mut io = TcpStream::connect(server.addr).await.unwrap();
        closed(&mut io).await;
    }

    #[tokio::test]
    async fn partial_http2_preface_cannot_extend_deadline_by_trickling() {
        let server = start_http().await;
        let io = TcpStream::connect(server.addr).await.unwrap();
        let (mut read, mut write) = io.into_split();
        let sender = tokio::spawn(async move {
            for byte in H2_PREFACE {
                if write.write_all(&[*byte]).await.is_err() {
                    break;
                }
                sleep(LIMIT / 4).await;
            }
        });
        // A timer reset by each byte would stay open for ~2.4 seconds.
        timeout(LIMIT * 3, closed(&mut read))
            .await
            .expect("preface deadline must not restart on each byte");
        sender.abort();
    }

    #[tokio::test]
    async fn incomplete_http1_headers_expire() {
        let server = start_http().await;
        let mut io = TcpStream::connect(server.addr).await.unwrap();
        io.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nX-Slow: ")
            .await
            .unwrap();
        closed(&mut io).await;
    }

    #[tokio::test]
    async fn keep_alive_is_reusable_then_expires_after_the_response() {
        let server = start_http().await;
        let mut io = TcpStream::connect(server.addr).await.unwrap();
        request(&mut io, "/").await;
        assert_eq!(response(&mut io).await, b"ok");
        request(&mut io, "/peer").await;
        assert_eq!(
            response(&mut io).await,
            io.local_addr().unwrap().to_string().as_bytes()
        );
        closed(&mut io).await;
    }

    #[tokio::test]
    async fn upload_body_may_pause_longer_than_the_header_deadline() {
        let server = start_http().await;
        let mut io = TcpStream::connect(server.addr).await.unwrap();
        io.write_all(b"POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: 6\r\n\r\nabc")
            .await
            .unwrap();
        sleep(SLOW).await;
        io.write_all(b"def").await.unwrap();
        assert_eq!(response(&mut io).await, b"abcdef");
        closed(&mut io).await;
    }

    #[tokio::test]
    async fn handler_can_run_longer_than_the_header_deadline() {
        let server = start_http().await;
        let mut io = TcpStream::connect(server.addr).await.unwrap();
        request(&mut io, "/slow").await;
        assert_eq!(response(&mut io).await, b"slow");
        closed(&mut io).await;
    }

    #[tokio::test]
    async fn expect_continue_does_not_apply_header_deadline_to_the_body() {
        let server = start_http().await;
        let mut io = TcpStream::connect(server.addr).await.unwrap();
        io.write_all(b"POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: 3\r\nExpect: 100-continue\r\n\r\n")
            .await
            .unwrap();
        let interim = b"HTTP/1.1 100 Continue\r\n\r\n";
        let mut received = vec![0; interim.len()];
        timeout(TEST_BOUND, io.read_exact(&mut received))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received, interim);
        sleep(SLOW).await;
        io.write_all(b"abc").await.unwrap();
        assert_eq!(response(&mut io).await, b"abc");
        closed(&mut io).await;
    }

    #[tokio::test]
    async fn paced_response_and_pipelined_request_survive_the_deadline() {
        let server = start_http().await;
        let mut io = TcpStream::connect(server.addr).await.unwrap();
        io.write_all(b"GET /stream HTTP/1.1\r\nHost: localhost\r\n\r\nGET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        assert_eq!(response(&mut io).await, b"abcdef");
        assert_eq!(response(&mut io).await, b"ok");
        closed(&mut io).await;
    }

    #[tokio::test]
    async fn complete_http2_preface_without_ping_ack_expires() {
        let server = start_http().await;
        let mut io = TcpStream::connect(server.addr).await.unwrap();
        io.write_all(H2_PREFACE).await.unwrap();
        closed(&mut io).await;
    }

    async fn http2_roundtrip(io: impl AsyncRead + AsyncWrite + Unpin + Send + 'static) {
        let (mut sender, connection) =
            hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(io))
                .await
                .unwrap();
        let driver = tokio::spawn(connection);
        // The driver acknowledges pings while idle; an application timeout
        // would wrongly kill this healthy reusable HTTP/2 connection.
        sleep(SLOW).await;
        let response = timeout(
            TEST_BOUND,
            sender.send_request(
                hyper::Request::builder()
                    .uri("https://localhost/stream")
                    .body(Body::empty())
                    .unwrap(),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(response.status(), 200);
        let body = timeout(TEST_BOUND, to_bytes(Body::new(response.into_body()), 1024))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&body[..], b"abcdef");
        driver.abort();
    }

    #[tokio::test]
    async fn responsive_http2_keeps_working_while_idle_and_streaming() {
        let server = start_http().await;
        http2_roundtrip(TcpStream::connect(server.addr).await.unwrap()).await;
    }

    async fn start_https(http2: bool) -> (RunningServer, tokio_rustls::TlsConnector) {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let config = axum_server::tls_rustls::RustlsConfig::from_der(
            vec![cert.cert.der().to_vec()],
            cert.signing_key.serialize_der(),
        )
        .await
        .unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        let mut client = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        client.alpn_protocols = vec![if http2 {
            b"h2".to_vec()
        } else {
            b"http/1.1".to_vec()
        }];
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = axum_server::Handle::new();
        let server = axum_server::Server::from_listener(listener)
            .acceptor(axum_server::tls_rustls::RustlsAcceptor::new(config));
        let server = with_timeout(server, LIMIT).handle(handle.clone());
        tokio::spawn(async move {
            server
                .serve(app().into_make_service_with_connect_info::<SocketAddr>())
                .await
                .unwrap();
        });
        (RunningServer { addr, handle }, connector)
    }

    async fn tls_connect(
        server: &RunningServer,
        connector: &tokio_rustls::TlsConnector,
    ) -> tokio_rustls::client::TlsStream<TcpStream> {
        connector
            .connect(
                rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                TcpStream::connect(server.addr).await.unwrap(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn https_silence_after_handshake_expires() {
        let (server, connector) = start_https(false).await;
        let mut io = tls_connect(&server, &connector).await;
        closed(&mut io).await;
    }

    #[tokio::test]
    async fn https_streaming_and_peer_address_are_preserved() {
        let (server, connector) = start_https(false).await;
        let mut io = tls_connect(&server, &connector).await;
        request(&mut io, "/peer").await;
        assert_eq!(
            response(&mut io).await,
            io.get_ref().0.local_addr().unwrap().to_string().as_bytes()
        );
        request(&mut io, "/stream").await;
        assert_eq!(response(&mut io).await, b"abcdef");
        closed(&mut io).await;
    }

    #[tokio::test]
    async fn https_http2_keeps_working_while_idle_and_streaming() {
        let (server, connector) = start_https(true).await;
        let io = tls_connect(&server, &connector).await;
        assert_eq!(io.get_ref().1.alpn_protocol(), Some(&b"h2"[..]));
        http2_roundtrip(io).await;
    }

    #[tokio::test]
    async fn empty_read_buffer_does_not_disarm_protocol_deadline() {
        let (server, _client) = tokio::io::duplex(64);
        let mut io = ProtocolIo::new(server, LIMIT);
        let mut empty = ReadBuf::new(&mut []);
        futures::future::poll_fn(|cx| Pin::new(&mut io).poll_read(cx, &mut empty))
            .await
            .unwrap();
        sleep(LIMIT * 2).await;
        let error = io.read_u8().await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }
}
