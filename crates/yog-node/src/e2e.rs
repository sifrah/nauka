//! Client HTTP chiffré de bout en bout : `upload` chiffre AVANT d'envoyer,
//! `download` déchiffre après réception. Les nœuds ne voient que du
//! ciphertext ; la clé vit dans le fragment du lien (`#...`), que le
//! protocole HTTP ne transmet jamais aux serveurs.

use std::path::Path;

use anyhow::{bail, Context, Result};
use tokio_stream::StreamExt;
use yog_crypto::FileKey;

#[derive(serde::Deserialize)]
struct UploadResponse {
    hash: String,
    size: u64,
}

/// Chiffre `file` puis l'uploade sur `api` (ex: http://1.2.3.4:8080).
/// Affiche le lien de partage complet, clé comprise.
pub async fn upload(api: &str, file: &Path, public_name: Option<String>) -> Result<()> {
    let api = api.trim_end_matches('/');
    let key = FileKey::generate();

    // Chiffrement streaming vers un fichier temporaire (mémoire bornée).
    let tmp = tempfile_path(file)?;
    {
        let key = key.clone();
        let src = file.to_path_buf();
        let dst = tmp.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut input = std::fs::File::open(&src)
                .with_context(|| format!("lecture de {}", src.display()))?;
            let mut output = std::io::BufWriter::new(std::fs::File::create(&dst)?);
            yog_crypto::encrypt(&key, &mut input, &mut output)?;
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
        // Par défaut, AUCUN nom en clair ne part au serveur (métadonnée).
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
            bail!("upload refusé ({}): {}", resp.status(), resp.text().await.unwrap_or_default());
        }
        let up: UploadResponse = resp.json().await?;
        Ok::<_, anyhow::Error>((up, ct_len))
    }
    .await;
    let _ = tokio::fs::remove_file(&tmp).await;
    let (up, ct_len) = result?;

    let plain_len = std::fs::metadata(file)?.len();
    println!("chiffré : {plain_len} octets → {ct_len} (AES-256-GCM, clé jamais transmise)");
    println!("stocké  : {} ({} octets de ciphertext)", up.hash, up.size);
    println!();
    println!("lien de partage (la partie après # est la clé — elle ne quitte jamais le client) :");
    println!("{api}/f/{}#{}", up.hash, key.encode());
    Ok(())
}

/// Télécharge un lien `http://…/f/<hash>#<clé>` et déchiffre vers `output`.
pub async fn download(link: &str, output: &Path) -> Result<()> {
    let (url, key) = match link.split_once('#') {
        Some((url, frag)) if !frag.is_empty() => (url, FileKey::decode(frag)?),
        _ => bail!(
            "lien sans clé (#…) — sans elle le contenu est indéchiffrable. \
             Lien complet requis, ex: http://hôte:8080/f/<hash>#<clé>"
        ),
    };

    let resp = reqwest::get(url).await.with_context(|| format!("GET {url}"))?;
    if !resp.status().is_success() {
        bail!("téléchargement refusé ({}): {}", resp.status(), resp.text().await.unwrap_or_default());
    }

    // Déchiffrement streaming : les octets réseau alimentent un pipe lu par
    // le décodeur synchrone dans une tâche bloquante.
    let (reader, mut writer) = std::io::pipe()?;
    let out_path = output.to_path_buf();
    let decrypt_task = tokio::task::spawn_blocking(move || -> Result<()> {
        let mut reader = std::io::BufReader::new(reader);
        let mut out = std::io::BufWriter::new(std::fs::File::create(&out_path)?);
        yog_crypto::decrypt(&key, &mut reader, &mut out)?;
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
                    break; // le décodeur a refusé (erreur d'auth) — il dira pourquoi
                }
            }
            Err(e) => {
                net_err = Some(e);
                break;
            }
        }
    }
    drop(writer); // EOF pour le décodeur
    decrypt_task.await?.map_err(|e| {
        anyhow::anyhow!("{e:#}{}", net_err.map(|n| format!(" (réseau: {n})")).unwrap_or_default())
    })?;

    println!(
        "déchiffré et vérifié : {} octets → {}",
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
