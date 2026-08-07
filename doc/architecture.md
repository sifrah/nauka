# Architecture

## Les crates

Le workspace est découpé en couches strictes — chaque crate ne connaît que
celles d'en dessous :

| Crate | Rôle | Dépend de |
|---|---|---|
| `yog-erasure` | Cœur pur (zéro I/O) : encodage Reed-Solomon par stripes, reconstruction, intégrité BLAKE3 | — |
| `yog-store` | Stockage disque d'un nœud : shards content-addressed, manifests JSON | yog-erasure |
| `yog-transport` | QUIC inter-nœuds (quinn) : protocole shards/manifests/Raft, mTLS, tuning débit | yog-erasure, yog-store |
| `yog-raft` | Consensus openraft : registre des fichiers + membership répliqués, stockage durable redb | yog-erasure, yog-transport |
| `yog-cluster` | Logique de cluster : placement rendezvous-hash, auto-healing, GC de rebalancement | yog-erasure, yog-store, yog-transport |
| `yog-discovery` | Rendez-vous DHT Mainline (pkarr) : publication/résolution des seeds, détection d'IP publique | — (pkarr, mainline) |
| `yog-node` | Le binaire : CLI, serveur, API HTTP, orchestration de tout ce qui précède | toutes |

## Les invariants du système

1. **L'intégrité est vérifiée à chaque frontière.** Chaque shard a un hash
   BLAKE3 ; chaque fichier a un hash global. Un shard est revérifié à chaque
   lecture disque, écarté s'il ne correspond pas (traité comme perdu, jamais
   utilisé silencieusement), et le fichier reconstruit est revérifié contre
   le hash du manifest avant d'être rendu.
2. **Le placement est une fonction pure.** « Qui doit détenir le shard i de
   la stripe s du fichier f ? » se calcule à partir de (hash du fichier,
   indices, liste triée des membres) — même réponse sur tous les nœuds, sans
   aucune coordination. Toute la convergence du cluster (healing, GC,
   rebalancement) découle de cet invariant.
3. **Le consensus ne transporte que des métadonnées.** Le log Raft réplique
   le registre des manifests et le membership — jamais les octets des
   shards, qui voyagent en direct par QUIC. Le consensus reste léger quel
   que soit le volume stocké.
4. **Le contenu est l'adresse.** Un shard est stocké sous son hash
   (content-addressed) : écriture idempotente, dédup gratuite, renvoi sans
   risque.
5. **Découverte ≠ admission.** La DHT publique donne les adresses ; le mTLS
   avec la clé de cluster décide qui entre. Un inconnu peut trouver le
   cluster, pas le rejoindre.
6. **L'identité se prouve.** Le node-id Raft est dérivé de la clé publique
   Ed25519 du nœud (8 premiers octets de blake3(pubkey)) — pas décrété par
   une option de CLI.

## Flux d'un upload (`POST /api/upload` sur n'importe quel nœud)

```
client ──POST /api/upload──▶ nœud N (n'importe lequel)
  1. N bufferise le flux dans data-dir/tmp en hashant au fil de l'eau
     (le placement est keyé sur le hash final du fichier)
  2. N relit le buffer stripe par stripe (4 Mio de données par défaut) :
       encode_stripe → k=4 shards de données + m=2 de parité (1 Mio chacun)
       pour chaque shard : owner = HRW(file_hash, stripe, index, membres)
         owner == N   → écriture locale
         owner == autre → put_shard QUIC vers lui (retry, idempotent)
  3. N écrit le manifest localement puis l'enregistre dans le registre
     Raft (write local si leader, sinon transmis au leader)
  4. réponse: { hash, size, name, link: "/f/<hash>" }
```

## Flux d'un download (`GET /f/{hash}` sur n'importe quel nœud)

```
client ──GET /f/<hash>──▶ nœud N
  1. manifest : store local, sinon registre répliqué en mémoire
  2. pour chaque stripe (streaming, une stripe en mémoire à la fois) :
       pour chaque shard : local ? sinon get_shard chez chaque membre
       (timeouts ; un pair injoignable est mémorisé et plus recontacté)
       decode_stripe : ≥ k shards valides suffisent — les manquants et
       les corrompus sont reconstruits par Reed-Solomon
  3. hash global recalculé au fil de l'eau, comparé au manifest
```

## Boucle de fond d'un nœud (mode consensus)

Toutes les `--scrub-interval` secondes (défaut 30 s) :

1. **Matérialisation** : les manifests présents dans le registre Raft mais
   absents du store local y sont écrits (un nœud qui a raté un upload le
   rattrape).
2. **Scrub (acquisition)** : pour chaque shard dont ce nœud est propriétaire
   selon le placement — manquant ou corrompu ? → collecte ≥ k shards de la
   stripe dans le cluster, décode, ré-encode, vérifie le hash, stocke.
3. **GC (libération)** : pour chaque shard local dont ce nœud n'est plus
   propriétaire (la vue a changé) — suppression uniquement après que TOUS
   les propriétaires actuels ont confirmé détenir leur copie.

Ces trois étapes rendent tout changement de topologie automatique : nœud
mort → ses shards sont régénérés ailleurs ; nœud ajouté → il acquiert sa
part et les autres libèrent la leur ; nœud retiré → le cluster ré-absorbe.
