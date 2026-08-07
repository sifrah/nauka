# Backlog

Les chantiers envisagés, classés. Deux sections : les **innovations** (ce
qui différencie le produit) et les **consolidations** (dettes techniques et
features attendues). Les items sont triés par priorité recommandée à
l'intérieur de chaque section.

## Innovations

### 1. Streaming vidéo chiffré avec seek — *prochain chantier recommandé*
Regarder une vidéo que le serveur ne peut pas lire, en sautant à
n'importe quel instant. Le chiffrement en chunks AES-GCM de 1 Mio le
permet structurellement : chaque chunk est déchiffrable indépendamment
(nonce = compteur), donc « minute 42 » = calcul de l'index de chunk +
HTTP Range sur le ciphertext + déchiffrement local du chunk seul.
Mega est quasiment seul au monde à offrir ça. Effort : moyen (Range sur
`GET /f/{hash}` aligné sur les chunks, lecteur côté client).
**Impact produit : maximal** — c'est la démo qui fait comprendre le
produit en 10 secondes.

### 2. Attestation de stockage (challenge-réponse)
Un nœud peut prétendre stocker ses shards et les avoir perdus. Challenge
périodique entre nœuds : `blake3(shard ‖ nonce)` — impossible à répondre
sans détenir réellement les octets, coût quasi nul. Version légère de ce
que Filecoin fait en preuves ZK lourdes. Ferme la boucle du placement
pondéré (capacité déclarée → capacité honorée). Effort : faible.

### 3. Géo-placement sans GeoIP : coordonnées Vivaldi
Les nœuds calculent des coordonnées virtuelles à partir des RTT mesurés
entre eux (algorithme Vivaldi) ; le placement maximise la distance entre
les shards d'une stripe → « tes fichiers survivent à la perte d'une
région », sans base MaxMind, sans configuration, auto-calibré — tout se
dérive, comme le reste. Bonus : le download se dirige vers le nœud le
plus proche. Vivaldi + placement erasure n'existe dans aucun produit
grand public. Effort : moyen. Héritage ChainRage (UUIDv8 géographique),
en mieux.

### 4. Upload/download direct-aux-shards (façon torrent)
Le client (CLI puis navigateur en wasm) fait l'encodage Reed-Solomon
lui-même et pousse chaque shard directement à son propriétaire, en
parallèle — le gateway n'enregistre que le manifest. Symétrique au
download (tirer de 4 nœuds à la fois). Upload à la vitesse cumulée du
cluster, pas d'un serveur. Architecture Storj, inexistante en
self-hosted. Effort : moyen-haut (protocole d'autorisation d'écriture
directe, wasm RS pour le navigateur).

### 5. Peering de clusters — le « BGP du stockage »
Deux clusters indépendants signent un accord et hébergent mutuellement de
la parité supplémentaire l'un de l'autre. Le E2E permet de confier des
octets illisibles à un pair qu'on n'a pas besoin de croire ; en échange,
survie à un désastre total local. Reprise après sinistre mutualisée,
sans contrat, sans blockchain. Catégorie inexistante. Effort : haut
(fédération d'identités, placement inter-clusters, comptabilité).

### 6. Le cluster héberge sa propre UI
L'interface web est stockée comme un fichier dans le cluster ; chaque
nœud la sert. Zéro déploiement frontend, mise à jour = un upload signé
de l'opérateur. Effort : faible (après l'UI elle-même). Conceptuellement
pur — « le stockage distribué qui se sert lui-même ».

### 7. Re-striping adaptatif
Ré-encoder les fichiers existants vers un autre schéma k+m quand le
cluster change d'échelle (ex. 4+2 → 8+3 à 11+ nœuds : plus de tolérance
pour moins de surcoût). Relire → ré-encoder → nouvelle version au
registre → GC de l'ancienne. Les grands systèmes (Ceph, MinIO) ne le
font pas non plus — profil figé par pool. Effort : moyen. À déclencher
manuellement d'abord (`cluster-restripe`), automatisable ensuite.

### 8. Transport Tor optionnel (arti)
Accès .onion embarqué en pur Rust via arti, en transport enfichable
(`--tor`) — jamais en dépendance obligatoire. Sert le créneau
anti-censure sans pénaliser le produit principal. Yggdrasil : écarté
(pas d'implémentation Rust, sidecar Go = le bazar ChainRage qu'on a
éliminé) ; son bénéfice réel (nœuds sans IP publique) passe par le NAT
traversal natif (voir consolidations).

## Consolidations

### A. UI web (drag & drop, lien de partage, lecteur)
La porte d'entrée grand public. Chiffrement E2E systématique en WebCrypto
(AES-GCM choisi pour ça — déchiffrement navigateur sans wasm). Se combine
avec l'innovation 1 (lecteur vidéo) et 6 (servie par le cluster).

### B. Suppression et expiration des fichiers
`UnregisterManifest` existe côté Raft ; il manque : DELETE sur l'API,
TTL optionnel à l'upload, purge des manifests locaux absents du registre,
et GC des shards orphelins (aujourd'hui explicitement hors périmètre).

### C. NAT traversal natif (hole punching QUIC + relais)
Ouvre le produit aux machines de salon — le vrai marché self-hosted.
Rend Yggdrasil définitivement inutile. Effort : haut (signaling via la
DHT, relais optionnels à la iroh).

### D. Authentification et quotas sur l'API HTTP
Prérequis à toute exposition publique. Tokens d'upload, quotas par clé,
rate limiting. (En attendant : reverse proxy.)

### E. Garde-fou disque plein
Refus des écritures au-delà de ~95 % + débordement sur le suivant du
classement HRW ; le scrubber rapatrie quand la place revient.

### F. Émission de certificats hors-ligne
La clé de cluster ne quitte plus le poste d'admin ; chaque nœud reçoit
un certificat pré-signé. Réduit le blast radius d'un nœud compromis.

### G. Fair queuing entre uploads concurrents
Les gros flux affament les petits (observé au stress test 15 Go — sans
danger, juste inéquitable). Ordonnancement par connexion côté serveur.

### H. Préparation open source
Nom définitif (chainrage vs nauka vs autre — vérification de
disponibilité entamée), README vitrine avec le kill-demo en GIF, CI
GitHub Actions, licence, merge de `empty` vers `main`.
