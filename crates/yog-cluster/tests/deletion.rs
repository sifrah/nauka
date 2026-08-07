//! Suppression : purge des manifests retirés du registre et des shards
//! devenus orphelins — sans jamais toucher à ce qui est encore référencé.

use std::collections::BTreeSet;
use std::sync::Arc;

use yog_cluster::healer::purge_deleted;
use yog_erasure::{encode_file, ErasureConfig};
use yog_store::ShardStore;

fn store_with_files(n: usize) -> (Arc<ShardStore>, Vec<yog_erasure::FileManifest>, tempfile::TempDir)
{
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ShardStore::open(dir.path()).unwrap());
    let cfg = ErasureConfig { data_shards: 2, parity_shards: 1, shard_size: 1024 };
    let mut manifests = Vec::new();
    for i in 0..n {
        // Contenus réellement distincts : un fichier de zéros produirait des
        // shards identiques au padding des autres (dédup content-addressed),
        // et plus rien ne serait orphelin.
        let data: Vec<u8> = (0..3000u32)
            .map(|b| ((b.wrapping_mul(2654435761)) ^ (i as u32 + 1)) as u8)
            .collect();
        let (manifest, stripes) = encode_file(&data, &cfg).unwrap();
        store.put_manifest(&manifest).unwrap();
        for stripe in &stripes {
            for shard in stripe {
                store.put_shard(&shard.data).unwrap();
            }
        }
        manifests.push(manifest);
    }
    (store, manifests, dir)
}

#[test]
fn purge_removes_deleted_files_and_their_shards() {
    let (store, manifests, _dir) = store_with_files(3);
    let shards_before = store.list_shards().unwrap().len();
    assert_eq!(store.list_manifests().unwrap().len(), 3);

    // Le registre ne connaît plus le premier fichier (supprimé).
    let live: BTreeSet<String> =
        manifests[1..].iter().map(|m| m.file_hash.clone()).collect();
    let report = purge_deleted(&store, &live, true).unwrap();

    assert_eq!(report.manifests_purged, 1);
    assert!(report.orphans_purged > 0, "les shards du fichier supprimé doivent partir");
    assert_eq!(store.list_manifests().unwrap().len(), 2);

    // Les fichiers restants sont intacts, shard par shard.
    for m in &manifests[1..] {
        assert!(store.get_manifest(&m.file_hash).is_ok());
        for stripe in &m.stripes {
            for hash in &stripe.shard_hashes {
                assert!(store.get_shard(hash).is_ok(), "shard vivant supprimé à tort");
            }
        }
    }
    assert!(store.list_shards().unwrap().len() < shards_before);

    // Idempotent : une seconde passe ne fait plus rien.
    let again = purge_deleted(&store, &live, true).unwrap();
    assert_eq!(again.manifests_purged, 0);
    assert_eq!(again.orphans_purged, 0);
}

#[test]
fn purge_is_inert_when_registry_is_not_ready() {
    // Un nœud fraîchement démarré, dont le registre est encore vide, ne
    // doit RIEN effacer — sinon il détruirait tout le cluster.
    let (store, _manifests, _dir) = store_with_files(2);
    let before_shards = store.list_shards().unwrap().len();
    let before_manifests = store.list_manifests().unwrap().len();

    let report = purge_deleted(&store, &BTreeSet::new(), false).unwrap();
    assert_eq!(report.manifests_purged, 0);
    assert_eq!(report.orphans_purged, 0);
    assert_eq!(store.list_shards().unwrap().len(), before_shards);
    assert_eq!(store.list_manifests().unwrap().len(), before_manifests);
}

#[test]
fn shards_shared_by_two_files_survive_one_deletion() {
    // Deux fichiers identiques partagent leurs shards (content-addressed) :
    // supprimer l'un ne doit pas casser l'autre.
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ShardStore::open(dir.path()).unwrap());
    let cfg = ErasureConfig { data_shards: 2, parity_shards: 1, shard_size: 1024 };
    let data = vec![42u8; 3000];
    let (m1, stripes) = encode_file(&data, &cfg).unwrap();
    // Même contenu, nom différent → même hash de fichier et mêmes shards.
    let mut m2 = m1.clone();
    m2.file_hash = format!("{}ff", &m1.file_hash[..62]); // hash distinct, shards partagés
    store.put_manifest(&m1).unwrap();
    store.put_manifest(&m2).unwrap();
    for stripe in &stripes {
        for shard in stripe {
            store.put_shard(&shard.data).unwrap();
        }
    }

    // m1 supprimé, m2 vivant : les shards partagés doivent rester.
    let live: BTreeSet<String> = [m2.file_hash.clone()].into_iter().collect();
    let report = purge_deleted(&store, &live, true).unwrap();
    assert_eq!(report.manifests_purged, 1);
    assert_eq!(report.orphans_purged, 0, "shards encore référencés par m2");
    for stripe in &m2.stripes {
        for hash in &stripe.shard_hashes {
            assert!(store.get_shard(hash).is_ok());
        }
    }
}
