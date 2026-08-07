//! The web interface, embedded in the binary.
//!
//! A node ships its own UI: `curl | sh` then `nauka serve` gives a working
//! interface with nothing else to deploy — which is the whole point of a
//! single binary. `--webui <dir>` still serves from disk, for front-end
//! development with `npm run dev` output.
//!
//! The assets are compiled in from `webui/dist`. If that directory is
//! missing at build time the binary still builds and simply has no UI, so
//! `cargo build` works on a fresh clone before `npm run build` has run.

use axum::body::Body;
use axum::http::{header, StatusCode, Uri};
use axum::response::{IntoResponse, Response};

#[derive(rust_embed::Embed)]
#[folder = "$CARGO_MANIFEST_DIR/../../webui/dist"]
#[allow_missing = true]
struct Assets;

/// Is a UI compiled into this binary?
pub fn is_embedded() -> bool {
    Assets::get("index.html").is_some()
}

/// Serves an embedded asset, falling back to `index.html` so that the
/// client-side routes (`/files`, `/dashboard`, `/d/<hash>`, `/w/<hash>`)
/// resolve on a hard refresh.
pub async fn serve(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    match Assets::get(path).or_else(|| Assets::get("index.html")) {
        Some(asset) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            // Fingerprinted assets are immutable; index.html must not be
            // cached or a redeploy would keep serving the old app shell.
            let cache = if path.starts_with("assets/") {
                "public, max-age=31536000, immutable"
            } else {
                "no-cache"
            };
            (
                [
                    (header::CONTENT_TYPE, mime.as_ref()),
                    (header::CACHE_CONTROL, cache),
                ],
                Body::from(asset.data.into_owned()),
            )
                .into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            "no web interface in this build — rebuild after `cd webui && npm run build`, \
             or point --webui at a dist directory",
        )
            .into_response(),
    }
}
