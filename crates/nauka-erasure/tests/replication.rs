//! Small-file replication (`data_shards = 1`): Reed-Solomon degenerates
//! into n copies where ANY ONE reconstructs — the whole point of routing
//! small files this way is the one-round-trip read.

use nauka_erasure::{decode_stripe, encode_file, ErasureConfig};

#[test]
fn replicated_small_file_decodes_from_any_single_copy() {
    let cfg = ErasureConfig::default().replicated_for(4096);
    assert_eq!(cfg.data_shards, 1);
    assert_eq!(cfg.shard_size, 4096);

    let data: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
    let (manifest, stripes) = encode_file(&data, &cfg).unwrap();
    assert_eq!(stripes.len(), 1, "one stripe");
    let copies = stripes[0].len();
    assert_eq!(copies, 1 + cfg.parity_shards, "1 data + m parity copies");

    // Any single surviving shard is enough — including parity-only.
    for keep in 0..copies {
        let mut slots: Vec<Option<Vec<u8>>> = vec![None; copies];
        slots[keep] = Some(stripes[0][keep].data.clone());
        let out = decode_stripe(slots, &manifest.stripes[0], &cfg).unwrap();
        assert_eq!(out, data, "copy {keep} alone reconstructs the file");
    }
}

#[test]
fn replication_tolerance_matches_the_wide_config() {
    // 1+2 loses any 2 of 3 and survives, like 4+2 loses any 2 of 6.
    let cfg = ErasureConfig::default().replicated_for(1000);
    let data = vec![42u8; 1000];
    let (manifest, stripes) = encode_file(&data, &cfg).unwrap();
    let mut slots: Vec<Option<Vec<u8>>> = stripes[0].iter().map(|s| Some(s.data.clone())).collect();
    slots[0] = None;
    slots[2] = None; // two of three gone
    let out = decode_stripe(slots, &manifest.stripes[0], &cfg).unwrap();
    assert_eq!(out, data);
}
