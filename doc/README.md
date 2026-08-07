# Documentation

Deux projets, une base de code aujourd'hui :

- **Nauka** — le **moteur** : un binaire Rust unique qui forme un cluster
  auto-organisé, découpe chaque fichier en shards Reed-Solomon dispersés
  sur les nœuds, et garantit l'intégrité de bout en bout. Quoi qu'il
  arrive (nœud mort, disque corrompu, région perdue), tant que k shards
  par stripe survivent quelque part, le fichier est reconstruit à
  l'identique, bit à bit. C'est ce qui sera ouvert en AGPL-3.0.
- **Yogfile** — le **service** de partage de fichiers bâti dessus :
  chiffrement de bout en bout, liens de partage, lecteur vidéo,
  interface web. Propulsé par Nauka.

Les crates `nauka-*` sont le moteur ; l'API HTTP, la webui et le
chiffrement client relèvent du service.

L'expérience opérateur tient en deux commandes :

```
nauka keygen --out nauka-keys        # une fois
nauka --keys ./nauka-keys serve      # sur chaque machine — la même commande
```

Les nœuds se découvrent via la DHT BitTorrent (Mainline), élisent un
fondateur si le cluster n'existe pas encore, s'authentifient mutuellement
(mTLS Ed25519), se répartissent les shards, se réparent et se rééquilibrent
en continu. Aucun serveur central, aucune infrastructure annexe, aucun
fichier de configuration.

## Sommaire

| Document | Contenu |
|---|---|
| [architecture.md](architecture.md) | Vue d'ensemble, crates, invariants, flux upload/download |
| [coeur-erasure.md](coeur-erasure.md) | Reed-Solomon, stripes, intégrité BLAKE3, stockage content-addressed |
| [transport.md](transport.md) | QUIC (quinn), protocole inter-nœuds, tuning débit |
| [consensus.md](consensus.md) | Raft (openraft), persistance, plan réseau dédié |
| [cluster.md](cluster.md) | Placement HRW, auto-healing, GC, membership à chaud |
| [identite-et-decouverte.md](identite-et-decouverte.md) | Clés de cluster, mTLS, node-id dérivé, DHT Mainline, élection de genèse |
| [api-http.md](api-http.md) | API publique : upload, download, listing |
| [chiffrement.md](chiffrement.md) | Bout en bout : AES-GCM côté client, la clé dans le fragment du lien |
| [operations.md](operations.md) | Déploiement, référence CLI, ports, dépannage, limites connues |
| [decisions.md](decisions.md) | Choix structurants et leçons des stress tests |
| [backlog.md](backlog.md) | Chantiers à venir : innovations et consolidations, priorisés |

## En un coup d'œil

```
                       ┌─────────── un nœud yogfile (un seul binaire) ───────────┐
  utilisateur ──HTTP──▶│ API :8080  ─┐                                            │
                       │             ▼                                            │
  autres nœuds ─QUIC──▶│ :7311 data ─┼─▶ nauka-erasure (Reed-Solomon k+m, BLAKE3)   │
   (mTLS Ed25519)      │             │   nauka-store  (shards content-addressed)    │
                       │ :7312 Raft ─┼─▶ nauka-raft   (openraft durable, redb)      │
  DHT Mainline ◀─UDP──▶│             └─▶ nauka-cluster (placement HRW, heal, GC)    │
   (découverte)        │                 nauka-discovery (pkarr, genèse, IP)        │
                       └──────────────────────────────────────────────────────────┘
```

## Vérifier que tout marche

```
cargo test            # 48 tests (unitaires + intégration, DHT locale incluse)
cargo test --release  # idem, optimisé (les tests raft/stress y sont plus rapides)

# Benchs transport (mesures de débit, non exécutés par défaut) :
cargo test -p nauka-transport --release --test bench -- --ignored --nocapture
```
