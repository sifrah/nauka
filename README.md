# Nauka

**Un moteur de stockage distribué qui se répare tout seul — un binaire, une
clé, zéro configuration.**

Nauka découpe chaque fichier en shards Reed-Solomon dispersés sur les nœuds
d'un cluster. Tant que `k` shards par stripe survivent quelque part, le
fichier est reconstruit **à l'identique, bit à bit** — nœud mort, disque
corrompu, région entière perdue.

Les nœuds se découvrent via la DHT BitTorrent, élisent un fondateur si le
cluster n'existe pas encore, s'authentifient mutuellement, se répartissent
les données selon leur capacité disque et leur distance réseau, se
réparent et se rééquilibrent en continu. Aucun serveur central, aucune
infrastructure annexe, aucun fichier de configuration.

```bash
nauka keygen --out ./nauka-keys      # une fois
scp -r nauka-keys vps:/etc/           # sur chaque machine
nauka --keys /etc/nauka-keys serve    # la même commande partout
```

C'est tout. Le cluster se forme.

## Ce qui le distingue

|  | Nauka | Garage | MinIO | IPFS |
|---|:---:|:---:|:---:|:---:|
| Erasure coding (pas de réplication ×3) | ✅ | ❌ | ✅ | ❌ |
| Auto-réparation | ✅ | partiel | ✅ | ❌ |
| Formation du cluster sans configuration | ✅ | ❌ | ❌ | ✅ |
| Binaire unique | ✅ | ✅ | ~ | ~ |
| Placement pondéré par capacité | ✅ | ✅ | ❌ | ❌ |
| Placement conscient de la topologie réseau | ✅ | ❌ | ❌ | ❌ |

**Durabilité.** 4+2 par défaut : chaque stripe survit à la perte de 2 shards
sur 6, pour +50 % de stockage — là où une réplication ×3 coûte +200 % pour
la même tolérance. Intégrité BLAKE3 vérifiée à chaque frontière : un shard
corrompu est détecté à la lecture et traité comme perdu, jamais servi.

**Zéro configuration.** L'identité d'un nœud est dérivée de sa clé publique
Ed25519. Son adresse est auto-détectée. Le cluster se trouve sur la DHT
Mainline sous une clé dérivée de la clé de cluster — rien d'autre à
distribuer, pas même une URL. Si aucun cluster n'existe, une élection de
genèse en désigne le fondateur, sans nœud privilégié.

**Placement intelligent.** Rendezvous hashing pondéré par la capacité
disque déclarée : tous les nœuds se remplissent au même pourcentage. Et les
nœuds apprennent leurs positions réseau à partir des RTT qu'ils mesurent
(coordonnées Vivaldi, sans base GeoIP) pour **écarter les shards d'une même
stripe** — un fichier survit à la perte d'une région, pas seulement d'une
machine.

**Preuves, pas déclarations.** Un nœud peut prétendre stocker ce qu'il a
perdu. Nauka exige des preuves de détention `blake3(nonce ‖ octets)` avant
toute libération de redondance, et audite ses pairs en continu par
échantillonnage.

## Démarrage

```bash
cargo build --release          # binaire dans target/release/nauka
cargo test                     # 48 tests (unitaires + intégration)
```

Déploiement, référence CLI et dépannage : [`doc/operations.md`](doc/operations.md).

## Documentation

| Document | Contenu |
|---|---|
| [architecture.md](doc/architecture.md) | Crates, invariants, flux upload/download |
| [coeur-erasure.md](doc/coeur-erasure.md) | Reed-Solomon, stripes, intégrité, stockage |
| [transport.md](doc/transport.md) | QUIC, protocole inter-nœuds, tuning débit |
| [consensus.md](doc/consensus.md) | Raft durable, plan réseau dédié |
| [cluster.md](doc/cluster.md) | Placement, healing, attestation, géo-placement |
| [identite-et-decouverte.md](doc/identite-et-decouverte.md) | mTLS, DHT, élection de genèse |
| [api-http.md](doc/api-http.md) | API publique, suppression, expiration |
| [chiffrement.md](doc/chiffrement.md) | Bout en bout, modèle de menace |
| [operations.md](doc/operations.md) | Déploiement, CLI, limites connues |
| [decisions.md](doc/decisions.md) | Choix structurants et leçons des stress tests |
| [backlog.md](doc/backlog.md) | Chantiers à venir |

## Yogfile

[Yogfile](https://github.com/sifrah/yogfile) est le service de partage de
fichiers bâti sur Nauka : chiffrement de bout en bout dans le navigateur,
liens de partage dont la clé ne quitte jamais le client, lecteur vidéo
chiffré avec seek. Son code vit aujourd'hui dans ce dépôt
(`crates/nauka-node/src/api.rs`, `webui/`) et en sera extrait.

## État

Jeune mais sérieux. Le socle est éprouvé par des tests d'intégration qui
tuent des processus, coupent l'alimentation du cluster entier, saturent le
réseau et corrompent des disques à dessein. Ce qui manque avant une mise en
production est listé sans détour dans
[operations.md](doc/operations.md#limites-connues-v1) et
[backlog.md](doc/backlog.md) — notamment l'authentification de l'API, la
traversée de NAT et une API S3.

## Licence

[AGPL-3.0](LICENSE). L'interface web dérive de la webui de
[ZeroFS](https://github.com/Barre/ZeroFS) (AGPL-3.0) — voir
[`webui/ATTRIBUTION.md`](webui/ATTRIBUTION.md).
