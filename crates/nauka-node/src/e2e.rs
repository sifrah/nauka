//! End-to-end encrypted HTTP client: `upload` encrypts BEFORE sending,
//! `download` decrypts after receiving. Nodes only ever see ciphertext;
//! the key lives in the link fragment (`#...`), which the HTTP protocol
//! never transmits to servers.

use std::path::Path;

use anyhow::{bail, Context, Result};
use tokio_stream::StreamExt;
use nauka_crypto::FileKey;

#[derive(serde::Deserialize)]
struct UploadResponse {
    hash: String,
    size: u64,
}

/// Encrypts `file` then uploads it to `api` (e.g. http://1.2.3.4:8080).
/// Prints the complete share link, key included.
pub async fn upload(api: &str, file: &Path, public_name: Option<String>) -> Result<()> {
    let api = api.trim_end_matches('/');
    let key = FileKey::generate();

    // Streaming encryption into a temporary file (bounded memory).
    let tmp = tempfile_path(file)?;
    {
        let key = key.clone();
        let src = file.to_path_buf();
        let dst = tmp.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut input = std::fs::File::open(&src)
                .with_context(|| format!("reading {}", src.display()))?;
            let mut output = std::io::BufWriter::new(std::fs::File::create(&dst)?);
            nauka_crypto::encrypt(&key, &mut input, &mut output)?;
            use std::io::Write;
            output.flush()?;
            Ok(())
        })
        .await??;
    }

    let result = async {
        let ct_file = tokio::fs::File::open(&tmp).await?;
        let ct_len = ct_file.metadata().await?.len();
        let body = reqwest::Body::wrap_stream(tokio_util::io::ReaderStream::new(ct_file));
        let mut url = format!("{api}/api/upload");
        // By default, NO plaintext name reaches the server (metadata).
        if let Some(name) = &public_name {
            url.push_str(&format!("?name={}", urlencode(name)));
        }
        let resp = reqwest::Client::new()
            .post(&url)
            .header(reqwest::header::CONTENT_LENGTH, ct_len)
            .body(body)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        if !resp.status().is_success() {
            bail!("upload refused ({}): {}", resp.status(), resp.text().await.unwrap_or_default());
        }
        let up: UploadResponse = resp.json().await?;
        Ok::<_, anyhow::Error>((up, ct_len))
    }
    .await;
    let _ = tokio::fs::remove_file(&tmp).await;
    let (up, ct_len) = result?;

    let plain_len = std::fs::metadata(file)?.len();
    println!("encrypted: {plain_len} bytes → {ct_len} (AES-256-GCM, key never transmitted)");
    println!("stored   : {} ({} bytes of ciphertext)", up.hash, up.size);
    println!();
    println!("share link (the part after # is the key — it never leaves the client):");
    println!("{api}/f/{}#{}", up.hash, key.encode());
    Ok(())
}

/// Downloads a `http://…/f/<hash>#<key>` link and decrypts it to `output`.
pub async fn download(link: &str, output: &Path) -> Result<()> {
    let (url, key) = match link.split_once('#') {
        Some((url, frag)) if !frag.is_empty() => (url, FileKey::decode(frag)?),
        _ => bail!(
            "link without a key (#…) — without it the content cannot be decrypted. \
             The complete link is required, e.g. http://host:8080/f/<hash>#<key>"
        ),
    };

    let resp = reqwest::get(url).await.with_context(|| format!("GET {url}"))?;
    if !resp.status().is_success() {
        bail!("download refused ({}): {}", resp.status(), resp.text().await.unwrap_or_default());
    }

    // Streaming decryption: the network bytes feed a pipe read by the
    // synchronous decoder running in a blocking task.
    let (reader, mut writer) = std::io::pipe()?;
    let out_path = output.to_path_buf();
    let decrypt_task = tokio::task::spawn_blocking(move || -> Result<()> {
        let mut reader = std::io::BufReader::new(reader);
        let mut out = std::io::BufWriter::new(std::fs::File::create(&out_path)?);
        nauka_crypto::decrypt(&key, &mut reader, &mut out)?;
        use std::io::Write;
        out.flush()?;
        Ok(())
    });

    let mut stream = resp.bytes_stream();
    let mut net_err = None;
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => {
                use std::io::Write;
                if writer.write_all(&bytes).is_err() {
                    break; // the decoder refused (auth error) — it will say why
                }
            }
            Err(e) => {
                net_err = Some(e);
                break;
            }
        }
    }
    drop(writer); // EOF for the decoder
    decrypt_task.await?.map_err(|e| {
        anyhow::anyhow!("{e:#}{}", net_err.map(|n| format!(" (network: {n})")).unwrap_or_default())
    })?;

    println!(
        "decrypted and verified: {} bytes → {}",
        std::fs::metadata(output)?.len(),
        output.display()
    );
    Ok(())
}

fn tempfile_path(source: &Path) -> Result<std::path::PathBuf> {
    let dir = std::env::temp_dir();
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let base = source.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    Ok(dir.join(format!("yog-e2e-{stamp:x}-{}", base.chars().take(32).collect::<String>())))
}

fn urlencode(s: &str) -> String {
    s.bytes()
        .flat_map(|b| {
            if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
                vec![b as char]
            } else {
                format!("%{b:02X}").chars().collect()
            }
        })
        .collect()
}
