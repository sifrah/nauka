# Documentation yogfile

yogfile est un serveur de fichiers distribué en Rust : un **binaire unique**
qui forme un cluster auto-organisé, découpe chaque fichier en shards
Reed-Solomon dispersés sur les nœuds, et garantit l'intégrité de bout en
bout — quoi qu'il arrive (nœud mort, disque corrompu, datacenter perdu),
tant que k shards par stripe survivent quelque part, le fichier est
reconstruit à l'identique, bit à bit.

L'expérience opérateur tient en deux commandes :

```
yog-node keygen --out yog-keys          # une fois
yog-node --keys ./yog-keys serve        # sur chaque machine — la même commande
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
  autres nœuds ─QUIC──▶│ :7311 data ─┼─▶ yog-erasure (Reed-Solomon k+m, BLAKE3)   │
   (mTLS Ed25519)      │             │   yog-store  (shards content-addressed)    │
                       │ :7312 Raft ─┼─▶ yog-raft   (openraft durable, redb)      │
  DHT Mainline ◀─UDP──▶│             └─▶ yog-cluster (placement HRW, heal, GC)    │
   (découverte)        │                 yog-discovery (pkarr, genèse, IP)        │
                       └──────────────────────────────────────────────────────────┘
```

## Vérifier que tout marche

```
cargo test            # 23 tests (unitaires + intégration, DHT locale incluse)
cargo test --release  # idem, optimisé (les tests raft/stress y sont plus rapides)

# Benchs transport (mesures de débit, non exécutés par défaut) :
cargo test -p yog-transport --release --test bench -- --ignored --nocapture
```
