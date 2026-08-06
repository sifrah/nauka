//! Nœud yogfile — v0 : CLI locale qui exerce le cœur (encode Reed-Solomon,
//! stockage content-addressed, reconstruction avec pertes, intégrité).
//! Les couches QUIC (transport inter-nœuds) et Raft (métadonnées cluster)
//! viendront se poser sur ces mêmes primitives.

use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use yog_erasure::{decode_file, encode_file, ErasureConfig};
use yog_store::ShardStore;

#[derive(Parser)]
#[command(name = "yog-node", about = "Serveur de fichiers distribué yogfile (cœur v0)")]
struct Cli {
    /// Répertoire de données du nœud.
    #[arg(long, default_value = "./yog-data")]
    data_dir: PathBuf,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Encode un fichier en shards Reed-Solomon et le stocke.
    Put {
        file: PathBuf,
        /// k : shards de données par stripe.
        #[arg(long, default_value_t = 4)]
        data_shards: usize,
        /// m : shards de parité par stripe (tolérance de perte).
        #[arg(long, default_value_t = 2)]
        parity_shards: usize,
    },
    /// Reconstruit un fichier depuis ses shards (tolère pertes/corruptions).
    Get {
        file_hash: String,
        #[arg(short, long)]
        output: PathBuf,
    },
    /// Vérifie qu'un fichier est reconstructible et intact.
    Verify { file_hash: String },
    /// Liste les fichiers stockés.
    List,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let store = ShardStore::open(&cli.data_dir)?;

    match cli.cmd {
        Cmd::Put { file, data_shards, parity_shards } => {
            let data = std::fs::read(&file)
                .with_context(|| format!("lecture de {}", file.display()))?;
            let cfg = ErasureConfig {
                data_shards,
                parity_shards,
                ..ErasureConfig::default()
            };
            let (manifest, stripes) = encode_file(&data, &cfg)?;
            let mut shard_count = 0;
            for stripe in &stripes {
                for shard in stripe {
                    store.put_shard(&shard.data)?;
                    shard_count += 1;
                }
            }
            store.put_manifest(&manifest)?;
            println!("stocké : {}", manifest.file_hash);
            println!(
                "  {} octets, {} stripes, {} shards ({}+{}), tolère la perte de {} shards/stripe",
                manifest.file_size,
                manifest.stripes.len(),
                shard_count,
                cfg.data_shards,
                cfg.parity_shards,
                cfg.parity_shards,
            );
        }
        Cmd::Get { file_hash, output } => {
            let data = reconstruct(&store, &file_hash)?;
            std::fs::write(&output, &data)?;
            println!("reconstruit : {} octets → {}", data.len(), output.display());
        }
        Cmd::Verify { file_hash } => {
            let manifest = store.get_manifest(&file_hash)?;
            let mut missing = 0usize;
            let mut total = 0usize;
            for stripe in &manifest.stripes {
                for hash in &stripe.shard_hashes {
                    total += 1;
                    if store.get_shard(hash).is_err() {
                        missing += 1;
                    }
                }
            }
            match reconstruct(&store, &file_hash) {
                Ok(_) => println!(
                    "OK : intègre et reconstructible ({missing}/{total} shards indisponibles)"
                ),
                Err(e) => bail!("IRRÉCUPÉRABLE ({missing}/{total} shards indisponibles) : {e}"),
            }
        }
        Cmd::List => {
            for hash in store.list_manifests()? {
                let m = store.get_manifest(&hash)?;
                println!("{hash}  {} octets", m.file_size);
            }
        }
    }
    Ok(())
}

/// Charge les shards disponibles (les manquants/corrompus deviennent `None`)
/// et laisse Reed-Solomon reconstruire.
fn reconstruct(store: &ShardStore, file_hash: &str) -> Result<Vec<u8>> {
    let manifest = store.get_manifest(file_hash)?;
    let stripes = manifest
        .stripes
        .iter()
        .map(|stripe| {
            stripe
                .shard_hashes
                .iter()
                .map(|hash| store.get_shard(hash).ok())
                .collect()
        })
        .collect();
    Ok(decode_file(&manifest, stripes)?)
}
