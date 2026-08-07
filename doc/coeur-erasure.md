# Cœur erasure coding et stockage

## nauka-erasure — le cœur pur (zéro I/O)

### Découpage en stripes

Un fichier est découpé en **stripes** de `data_shards × shard_size` octets
(défaut : 4 × 1 Mio = 4 Mio de données par stripe). Chaque stripe est
encodée indépendamment en `k` shards de données + `m` shards de parité
(Reed-Solomon sur GF(2⁸), crate `reed-solomon-erasure` avec SIMD).

- La dernière stripe est généralement partielle : zero-padding à
  l'encodage, `data_len` dans les métadonnées pour tronquer au décodage.
- Contrainte GF(2⁸) : `k + m ≤ 255`.
- Défaut cluster : **4+2** — chaque stripe survit à la perte de n'importe
  quels 2 shards sur 6, pour un surcoût de stockage de 50 %.

### Configuration

```rust
ErasureConfig { data_shards: 4, parity_shards: 2, shard_size: 1 Mio }
```

Définie au niveau du cluster (mêmes paramètres pour tous les fichiers),
embarquée dans chaque manifest — un changement de config ne casse pas les
fichiers existants, chacun porte la sienne.

### Le manifest

Toutes les métadonnées nécessaires pour reconstruire et prouver un fichier,
sans ses octets :

```
FileManifest {
  file_hash:  BLAKE3 du fichier complet (l'identifiant global)
  file_size:  taille réelle en octets
  name:       nom d'affichage optionnel (hors hash)
  config:     ErasureConfig utilisée
  stripes: [ { data_len, shard_hashes: [BLAKE3 de chaque shard] } … ]
}
```

### Reconstruction et intégrité

`decode_stripe(slots, meta, cfg)` prend un slot par shard
(`Some(octets)`/`None` si perdu) :

1. Chaque shard présent est vérifié contre son hash du manifest — un shard
   **corrompu est traité comme perdu** (jamais utilisé).
2. S'il reste ≥ k shards valides, Reed-Solomon reconstruit les manquants.
3. Les shards de données reconstruits sont revérifiés contre le manifest.
4. Sinon : erreur `NotEnoughShards { available, needed }` — échec propre,
   jamais de sortie corrompue.

`decode_file` enchaîne les stripes puis vérifie le hash global du fichier.
Propriété prouvée par les tests : perte de n'importe quels m shards par
stripe → reconstruction identique ; perte de m+1 → refus propre ;
corruption silencieuse → détectée et réparée.

## nauka-store — stockage disque d'un nœud

Layout du data-dir :

```
data-dir/
  shards/ab/cdef…      # content-addressed, fanout sur 2 hex du hash
  manifests/<hash>.json
  raft/                # log redb + snapshot (voir consensus.md)
  tmp/                 # buffers d'upload de l'API HTTP
  node.key             # identité Ed25519 du nœud (mode --keys)
```

Propriétés :

- **Content-addressed** : le hash EST le chemin. `put_shard` est idempotent
  et déduplique gratuitement (deux fichiers partageant un shard identique
  ne le stockent qu'une fois).
- **Écriture atomique** : fichier temporaire + `rename` — jamais de shard
  à moitié écrit visible.
- **Pas de fsync sur les shards** (choix mesuré : le fsync par shard de
  1 Mio divise le débit d'ingestion par ~20, et un shard perdu sur crash
  machine est exactement ce que le scrubber sait réparer). Les manifests,
  rares et précieux, restent fsyncés.
- **Vérification à chaque lecture** : `get_shard` recalcule le hash ; une
  corruption disque (bit rot) renvoie `CorruptShard`, jamais des octets
  faux.
