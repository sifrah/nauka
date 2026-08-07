# Transport QUIC inter-nœuds

## Protocole

Un échange = **un stream bidirectionnel QUIC** : le client écrit une
`Request`, le serveur répond une `Response`, framées `u32 LE longueur +
bincode` (taille max d'un message : 64 Mio). Les streams d'une même
connexion sont multiplexés — plusieurs shards transitent en parallèle sans
head-of-line blocking entre eux.

```
Request:  Ping | PutShard(bytes) | GetShard(hash) | HasShard(hash)
        | PutManifest(m) | GetManifest(hash) | Raft(RaftRpc)
Response: Pong | PutShardOk(hash) | Shard(Option<bytes>) | Has(bool)
        | PutManifestOk | Manifest(Option<m>) | Raft(bytes) | Error(str)
```

Détails de comportement :

- `GetShard` d'un shard **corrompu côté serveur → `None`** (comme absent) :
  le client le reconstruit par Reed-Solomon au lieu de recevoir des octets
  faux.
- Les RPCs Raft (`RaftRpc::{AppendEntries,Vote,InstallSnapshot,Admin}`)
  sont des payloads bincode opaques pour le transport, remis au moteur
  openraft local via le trait `RaftHandler` branché sur le serveur.
- ALPN : `yog/0`.

## Les deux plans réseau

Chaque nœud en mode consensus ouvre **deux endpoints QUIC** :

| Plan | Port | Buffers socket UDP | Rôle |
|---|---|---|---|
| Data | P (défaut 7311) | 8 Mio | shards, manifests, RPCs d'admin |
| Consensus | **P + 1** | 1 Mio | RPCs Raft exclusivement |

Pourquoi : sous saturation, les heartbeats Raft faisaient la queue derrière
des mégaoctets de shards dans le même socket → timeouts → ré-élections en
pleine charge (observé pendant le stress test 15 Go). Sockets séparés =
files kernel séparées ; les petits buffers du plan consensus **bornent le
délai de queue**. Le plan consensus **refuse toute requête non-Raft** : une
collision de ports ne peut pas le transformer en faux plan de données.

Conséquence opérationnelle : ouvrir **P et P+1 en UDP**, et espacer les
ports d'au moins 2 si plusieurs nœuds cohabitent sur un hôte.

Test de régression (`nauka-raft/tests/priority.rs`) : 2,2 Go injectés en 12 s
pendant des écritures registre — 0 changement de leader, 0 écriture échouée.

## Tuning débit (leçons du stress test 15 Go)

Le débit est passé de **6 Mo/s à ~120 Mo/s** par la levée successive de
quatre goulots, dans l'ordre où ils ont été découverts :

1. **`max_udp_payload_size`** (le vrai coupable, ×10) : le défaut quinn
   (1472 o) plafonnait le MTU quels que soient `initial_mtu` et la
   découverte. Porté à 65527 ; le MTU découvert atteint ~16k sur loopback,
   les jumbo frames en datacenter, et retombe proprement à 1200 sur
   Internet.
2. **BBR** au lieu de Cubic : sur lien rapide à petit buffer, Cubic
   s'effondre aux pertes (mesuré : 7 Mo/s, 5 495 pertes, RTT 526 ms de
   bufferbloat) ; BBR mesure le débit réel et pace ses envois. Fenêtre
   initiale 4 Mio.
3. **Buffers socket UDP 8 Mio** : le défaut macOS d'envoi est de… 9216
   octets.
4. **Keep-alive 2 s + idle timeout 30 s** explicites : une connexion
   silencieuse sous congestion ne meurt pas en douce ; les échecs sont
   francs et les retries (idempotents) reprennent.

Micro-benchs reproductibles :

```
cargo test -p nauka-transport --release --test bench -- --ignored --nocapture
# raw_quinn_single_stream   : débit quinn brut + stats de chemin (rtt, cwnd, mtu, pertes)
# raw_put_shard_throughput  : débit du protocole put_shard pipeliné
# single_put_shard_latency  : latence par taille de payload
```

## TLS

Deux modes, choisis au démarrage du process (voir
[identite-et-decouverte.md](identite-et-decouverte.md)) :

- **mTLS de cluster** (des clés sont fournies) : certificats Ed25519 signés
  par la clé de cluster, vérification mutuelle, SNI `node.nauka`.
- **Insecure** (aucune clé) : certificat auto-signé, client sans
  vérification — lien chiffré mais pairs non authentifiés. Conservé pour le
  développement, avec warning au démarrage.
