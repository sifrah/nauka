//! HTTPS with certificates the cluster obtains for itself.
//!
//! With `NAUKA_HTTPS_DOMAIN=cdn.example.com` set, every node serves the
//! full API over TLS on :443 and keeps its own Let's Encrypt
//! certificate fresh — no reverse proxy, no external DNS API, no shared
//! secret. The trick is that the cluster IS the authoritative DNS for
//! the domain (see `dns.rs`): a node publishes its ACME DNS-01 token
//! into the replicated state, the NS nodes serve it as TXT, the CA
//! reads it, the certificate lands. The cluster is its own CA plumbing.
//!
//! Each node orders a certificate for `{domain, <node-alias>.domain}` —
//! the per-node alias keeps the SAN set unique, which sidesteps Let's
//! Encrypt's duplicate-certificate limit (5/week for identical sets;
//! nine nodes would trip it on day one). Renewal runs daily and reloads
//! the listener in place under 30 days of validity.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};

use crate::api::ApiState;

/// Renew once validity drops under this many days.
const RENEW_UNDER_DAYS: i64 = 30;

/// The node's unique SAN alias: `n5-223-60-22.cdn.example.com`.
fn node_alias(state: &ApiState, domain: &str) -> String {
    let ip = state.self_id.split(':').next().unwrap_or("node");
    format!("n{}.{domain}", ip.replace(['.', ':'], "-"))
}

/// Serves HTTPS once a certificate exists, and keeps it fresh forever.
pub async fn run(state: Arc<ApiState>, domain: String, data_dir: PathBuf) {
    let dir = data_dir.join("tls");
    if let Err(e) = std::fs::create_dir_all(&dir) {
        eprintln!("https: cannot create {}: {e} — disabled", dir.display());
        return;
    }
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");

    let mut serving = false;
    let mut rustls_config: Option<axum_server::tls_rustls::RustlsConfig> = None;
    // Issuance failures back off exponentially: a raft write racing an
    // election deserves a fast retry, a real misconfiguration must not
    // burn Let's Encrypt's failed-validation budget (5/hour).
    let mut failures: u32 = 0;

    loop {
        let days = cert_days_left(&cert_path);
        if days.unwrap_or(-1) < RENEW_UNDER_DAYS {
            eprintln!(
                "https: certificate for {domain} {} — ordering from Let's Encrypt",
                match days {
                    Some(d) => format!("valid {d} more day(s)"),
                    None => "absent".into(),
                }
            );
            let backoff = std::time::Duration::from_secs(
                60u64.saturating_mul(2u64.pow(failures.min(6))).min(3600),
            );
            match issue(&state, &domain, &dir).await {
                Ok((cert_pem, key_pem)) => {
                    if std::fs::write(&cert_path, &cert_pem)
                        .and_then(|()| std::fs::write(&key_path, &key_pem))
                        .is_err()
                    {
                        failures += 1;
                        eprintln!(
                            "https: cannot persist the certificate — retrying in {}s",
                            backoff.as_secs()
                        );
                        tokio::time::sleep(backoff).await;
                        continue;
                    }
                    failures = 0;
                    eprintln!("https: certificate for {domain} issued");
                }
                Err(e) => {
                    failures += 1;
                    eprintln!(
                        "https: issuance failed ({e:#}) — retrying in {}s",
                        backoff.as_secs()
                    );
                    tokio::time::sleep(backoff).await;
                    continue;
                }
            }
        }

        // Load (or hot-reload) the certificate into the listener.
        match &rustls_config {
            Some(cfg) => {
                if cfg
                    .reload_from_pem_file(&cert_path, &key_path)
                    .await
                    .is_err()
                {
                    eprintln!("https: reload failed — keeping the previous certificate");
                }
            }
            None => {
                match axum_server::tls_rustls::RustlsConfig::from_pem_file(&cert_path, &key_path)
                    .await
                {
                    Ok(cfg) => {
                        let router = crate::api::router(state.clone());
                        let cfg2 = cfg.clone();
                        tokio::spawn(async move {
                            eprintln!("https: serving on :443");
                            let addr = std::net::SocketAddr::from(([0, 0, 0, 0], 443));
                            if let Err(e) = axum_server::bind_rustls(addr, cfg2)
                            .serve(
                                router
                                    .into_make_service_with_connect_info::<std::net::SocketAddr>(),
                            )
                            .await
                        {
                            eprintln!("https: server stopped: {e}");
                        }
                        });
                        rustls_config = Some(cfg);
                        serving = true;
                    }
                    Err(e) => {
                        eprintln!("https: unreadable certificate ({e}) — retrying in 1h");
                        tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                        continue;
                    }
                }
            }
        }
        let _ = serving;
        tokio::time::sleep(std::time::Duration::from_secs(86_400)).await;
    }
}

/// Days of validity left on the on-disk certificate, None if absent or
/// unreadable.
fn cert_days_left(cert_path: &std::path::Path) -> Option<i64> {
    // Certificates are issued for 90 days; we renew off the file's own
    // mtime (set at issuance). Simple, dependency-free, and immune to
    // clock drift games a parser would not be.
    let issued = std::fs::metadata(cert_path).ok()?.modified().ok()?;
    let age_days = issued.elapsed().ok()?.as_secs() as i64 / 86_400;
    Some(90 - age_days)
}

/// One full ACME order: account, DNS-01 via the cluster's own DNS,
/// certificate. Returns (chain PEM, key PEM) — the library generates
/// the key at finalize time.
async fn issue(
    state: &Arc<ApiState>,
    domain: &str,
    dir: &std::path::Path,
) -> Result<(String, String)> {
    use instant_acme::{
        Account, AuthorizationStatus, ChallengeType, Identifier, LetsEncrypt, NewAccount, NewOrder,
        OrderStatus, RetryPolicy,
    };

    // Account: reuse the stored credentials, else create (and store).
    let creds_path = dir.join("account.json");
    let account = if let Ok(bytes) = std::fs::read(&creds_path) {
        let creds = serde_json::from_slice(&bytes).context("stored ACME credentials")?;
        Account::builder()?.from_credentials(creds).await?
    } else {
        let (account, creds) = Account::builder()?
            .create(
                &NewAccount {
                    contact: &[],
                    terms_of_service_agreed: true,
                    only_return_existing: false,
                },
                LetsEncrypt::Production.url().to_owned(),
                None,
            )
            .await?;
        std::fs::write(&creds_path, serde_json::to_vec(&creds)?)?;
        account
    };

    let alias = node_alias(state, domain);
    let identifiers = [
        Identifier::Dns(domain.to_string()),
        Identifier::Dns(alias.clone()),
    ];
    let mut order = account.new_order(&NewOrder::new(&identifiers)).await?;

    // Publish every DNS-01 token through the cluster; remember the rows
    // for cleanup.
    let mut published: Vec<String> = Vec::new();
    {
        let mut authorizations = order.authorizations();
        let mut pending = Vec::new();
        while let Some(result) = authorizations.next().await {
            let mut authz = result?;
            match authz.status {
                AuthorizationStatus::Valid => continue,
                AuthorizationStatus::Pending => {}
                other => return Err(anyhow!("authorization in state {other:?}")),
            }
            let challenge = authz
                .challenge(ChallengeType::Dns01)
                .ok_or_else(|| anyhow!("no DNS-01 challenge offered"))?;
            let name = format!("_acme-challenge.{}", challenge.identifier()).to_ascii_lowercase();
            let value = challenge.key_authorization().dns_value();
            let resp = state
                .app
                .write(nauka_raft::types::AppCommand::SetAcmeTxt {
                    name: name.clone(),
                    node_addr: state.self_id.clone(),
                    value,
                })
                .await
                .context("publishing the challenge")?;
            if !resp.ok {
                return Err(anyhow!("the cluster refused the challenge record"));
            }
            published.push(name);
            pending.push(challenge.identifier().to_string());
        }
        // Replication to the NS nodes needs a moment before the CA looks.
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        let mut authorizations = order.authorizations();
        while let Some(result) = authorizations.next().await {
            let mut authz = result?;
            if authz.status != AuthorizationStatus::Pending {
                continue;
            }
            if let Some(mut challenge) = authz.challenge(ChallengeType::Dns01) {
                challenge.set_ready().await?;
            }
        }
        let _ = pending;
    }

    let status = order.poll_ready(&RetryPolicy::default()).await;
    // The rows served their purpose either way: clean up first.
    for name in &published {
        let _ = state
            .app
            .write(nauka_raft::types::AppCommand::ClearAcmeTxt {
                name: name.clone(),
                node_addr: state.self_id.clone(),
            })
            .await;
    }
    let status = status?;
    if status != OrderStatus::Ready {
        return Err(anyhow!("order did not become ready ({status:?})"));
    }

    let private_key_pem = order.finalize().await?;
    let cert_chain_pem = order.poll_certificate(&RetryPolicy::default()).await?;
    Ok((cert_chain_pem, private_key_pem))
}
