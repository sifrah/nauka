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
| Découverte DHT zéro-config + élection de genèse | `yog-discovery` |
| Identité crypto + mTLS de cluster | `yog-transport/tls` |
| Consensus Raft durable + plan réseau dédié | `yog-raft` |

## Innovations

### 1. Géo-placement sans GeoIP : coordonnées Vivaldi — *prochain chantier recommandé*
Les nœuds calculent des coordonnées virtuelles à partir des RTT mesurés
entre eux (algorithme Vivaldi) ; le placement maximise la distance entre
les shards d'une stripe → « tes fichiers survivent à la perte d'une
région », sans base MaxMind, sans configuration, auto-calibré — tout se
dérive, comme le reste. Bonus : le download se dirige vers le nœud le
plus proche. Vivaldi + placement erasure n'existe dans aucun produit
grand public. **Effort : moyen.** Héritage ChainRage (UUIDv8
géographique), en mieux.

### 2. Le cluster héberge sa propre UI
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

### A. Suppression et expiration des fichiers
`UnregisterManifest` existe côté Raft ; il manque : `DELETE` sur l'API,
TTL optionnel à l'upload, purge des manifests locaux absents du registre,
et GC des shards orphelins (aujourd'hui explicitement hors périmètre).
**Le plus attendu des utilisateurs.**

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
