# Backlog

Les chantiers, classés. Trois sections : ce qui est **livré**, les
**innovations** (ce qui différencie le produit) et les **consolidations**
(dettes et features attendues). Priorité décroissante dans chaque section.

## Livré

| Chantier | Où |
|---|---|
| Interface web (drag & drop, trousseau de clés, liens de partage) | `webui/`, dérivée de ZeroFS (AGPL-3.0) |
| Streaming vidéo chiffré avec seek | Service Worker `/stream/{hash}` + Range serveur |
| Chiffrement de bout en bout | `yog-crypto` (Rust) ↔ WebCrypto (navigateur), compat croisée prouvée |
| Placement pondéré par capacité disque | `yog-cluster/placement` (WRH) |
| Attestation de stockage (preuves de détention) | `yog-cluster/audit` + GC durci |
| Géo-placement sans GeoIP (coordonnées Vivaldi) | `yog-cluster/vivaldi` + `stripe_owners_geo` |
| Découverte DHT zéro-config + élection de genèse | `yog-discovery` |
| Identité crypto + mTLS de cluster | `yog-transport/tls` |
| Consensus Raft durable + plan réseau dédié | `yog-raft` |

## Innovations

### 1. API S3-compatible — *le multiplicateur d'adoption*
Exposer un sous-ensemble de l'API S3 fait entrer tout l'écosystème d'un
coup : rclone, restic, Velero, Terraform, les SDK AWS, les registries
Docker, Thanos/Loki. Un self-hoster pointe `restic` sur yogfile et obtient
une sauvegarde **chiffrée, erasure-codée, géo-répartie et auto-réparée**
en une commande. C'est la voie qu'a prise Garage — sauf que Garage
réplique ×3 (+200 %) là où nous faisons du 4+2 (+50 %), avec géo-placement
et attestation en plus.

Pas de conflit avec le zéro-connaissance : restic et rclone chiffrent
déjà côté client, la propriété « le serveur ne peut pas lire » est donc
préservée. Les deux mondes coexistent — S3 pour l'infra et les outils,
l'API native + webui pour le partage E2E grand public.

À construire :
1. **Indirection mutable** `(bucket, key) → file_hash` dans l'état Raft —
   seul vrai ajout sémantique (S3 écrase des clés, notre store est
   immuable et content-addressed).
2. Sous-ensemble utile : PUT/GET/HEAD/DELETE object, ListObjectsV2,
   CreateBucket, et **multipart upload** (les SDK en dépendent).
3. **SigV4** : la signature AWS règle du même coup la consolidation B —
   les access keys S3 *sont* le système d'authentification.

**Effort : moyen-haut** (1–2 sessions). **Prérequis : la suppression
(consolidation A)** — un `DELETE Object` qui ne supprime rien n'est pas
acceptable.

### 2. Le cluster héberge sa propre UI — *effort faible*
L'interface est aujourd'hui servie depuis `webui/dist` sur le disque de
chaque nœud. La stocker **comme un fichier dans le cluster** (upload signé
par l'opérateur, servi par n'importe quel nœud) supprime tout déploiement
frontend et rend la mise à jour atomique. **Effort : faible.**
Conceptuellement pur — « le stockage distribué qui se sert lui-même ».

### 3. Upload/download direct-aux-shards (façon torrent)
Le client (CLI puis navigateur en wasm) fait l'encodage Reed-Solomon
lui-même et pousse chaque shard directement à son propriétaire, en
parallèle — le gateway n'enregistre que le manifest. Symétrique au
download (tirer de 4 nœuds à la fois). Upload à la vitesse cumulée du
cluster, pas d'un serveur. Architecture Storj, inexistante en
self-hosted. **Effort : moyen-haut** (protocole d'autorisation d'écriture
directe, wasm RS pour le navigateur).

### 4. Peering de clusters — le « BGP du stockage »
Deux clusters indépendants signent un accord et hébergent mutuellement de
la parité supplémentaire l'un de l'autre. Le E2E permet de confier des
octets illisibles à un pair qu'on n'a pas besoin de croire ; en échange,
survie à un désastre total local. Reprise après sinistre mutualisée, sans
contrat, sans blockchain. Catégorie inexistante. **Effort : haut**
(fédération d'identités, placement inter-clusters, comptabilité). L'attestation, prérequis, est en place.

### 5. Re-striping adaptatif
Ré-encoder les fichiers existants vers un autre schéma k+m quand le
cluster change d'échelle (ex. 4+2 → 8+3 à 11+ nœuds : plus de tolérance
pour moins de surcoût). Relire → ré-encoder → nouvelle version au registre
→ GC de l'ancienne. Ceph et MinIO ne le font pas non plus (profil figé par
pool). **Effort : moyen.** Manuel d'abord (`cluster-restripe`),
automatisable ensuite.

### 6. Transport Tor optionnel (arti)
Accès .onion embarqué en pur Rust via arti, en transport enfichable
(`--tor`) — jamais en dépendance obligatoire. Sert le créneau anti-censure
sans pénaliser le produit principal. Yggdrasil : écarté (pas
d'implémentation Rust, sidecar Go = le bazar ChainRage qu'on a éliminé) ;
son bénéfice réel (nœuds sans IP publique) passe par le NAT traversal
natif (voir C).

## Consolidations

### A. Suppression, expiration et blocage par hash
`UnregisterManifest` existe côté Raft ; il manque : `DELETE` sur l'API,
TTL optionnel à l'upload, purge des manifests locaux absents du registre,
et GC des shards orphelins (aujourd'hui explicitement hors périmètre).
**Le plus attendu des utilisateurs — et prérequis à toute mise en ligne
publique.**

Même plomberie, à faire dans la foulée : **blocage par hash** (commande
Raft `BanHash`) — l'API refuse de servir un hash banni (`410 Gone`), son
manifest sort du registre, le GC purge ses shards. Permet d'honorer un
signalement ou une réquisition **sans jamais lire les contenus** (on agit
sur une empreinte), et de rejeter un ré-upload identique d'emblée. Limite
structurelle assumée : ne bloque que ce fichier à l'octet près — un
ré-upload chiffré avec une autre clé produit un autre hash. Prévoir aussi
un point de contact abus et un registre des demandes (cf.
[chiffrement.md](chiffrement.md) pour ce que l'opérateur peut/ne peut pas
fournir).

### B. Authentification et quotas sur l'API HTTP
Prérequis à toute exposition publique. Tokens d'upload, quotas par clé,
rate limiting. (En attendant : reverse proxy.)

### C. NAT traversal natif (hole punching QUIC + relais)
Ouvre le produit aux machines de salon — le vrai marché self-hosted. Rend
Yggdrasil définitivement inutile. **Effort : haut** (signaling via la DHT,
relais optionnels à la iroh).

### D. Garde-fou disque plein
Refus des écritures au-delà de ~95 % + débordement sur le suivant du
classement HRW ; le scrubber rapatrie quand la place revient.

### E. MediaSource Extensions (fMP4) pour le repli du lecteur
Le streaming par Service Worker couvre le cas nominal. Le repli
« déchiffrement complet en mémoire » (si le worker est indisponible) reste
plafonné à 600 Mo. MSE + fMP4 lèverait ce plafond et améliorerait la
compatibilité navigateurs.

### F. Émission de certificats hors-ligne
La clé de cluster ne quitte plus le poste d'admin ; chaque nœud reçoit un
certificat pré-signé. Réduit le blast radius d'un nœud compromis.

### G. Fair queuing entre uploads concurrents
Les gros flux affament les petits (observé au stress test 15 Go — sans
danger, juste inéquitable). Ordonnancement par connexion côté serveur.

### H. Préparation open source
- **Nom définitif** : `chainrage` (libre sur GitHub) vs `nauka`
  (homonymes existants, rien de bloquant) vs autre.
- **Licence** : tranché — dépôt entier en **AGPL-3.0** (`LICENSE` à la
  racine, `license = "AGPL-3.0-only"` dans le workspace Cargo).
- README vitrine avec le kill-demo en GIF (`rm -rf` d'un nœud → réparation
  automatique ; deux terminaux → cluster auto-formé).
- CI GitHub Actions (`cargo test`, `npm run build`), merge de `empty` vers
  `main`.
