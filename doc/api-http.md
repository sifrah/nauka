# API HTTP publique

Chaque nœud en mode consensus expose l'API (défaut `0.0.0.0:8080`,
réglable par `--http <addr>`, désactivable par `--no-http`). **N'importe
quel nœud est un point d'entrée complet** — upload, download et listing
donnent le même résultat partout.

L'API est aujourd'hui **sans authentification** (v1) : à exposer derrière
un reverse proxy si besoin, en attendant la couche comptes/quotas.

## `POST /api/upload?name=<nom>`

Corps : les octets bruts du fichier (`--data-binary` avec curl).

Le nœud bufferise le flux sur disque (`data-dir/tmp`, hash BLAKE3 calculé
au fil de l'eau), encode stripe par stripe, pousse chaque shard chez son
propriétaire HRW (lui-même inclus), écrit le manifest localement puis
l'enregistre dans le registre Raft (via le leader). Mémoire bornée à
quelques stripes quel que soit la taille du fichier.

```
curl -X POST --data-binary @video.mp4 \
  "http://node1:8080/api/upload?name=video.mp4"
```

Réponse `200` :

```json
{
  "hash": "988f6e61…",
  "size": 30000000,
  "name": "video.mp4",
  "stripes": 8,
  "data_shards": 4,
  "parity_shards": 2,
  "link": "/f/988f6e61…"
}
```

Erreurs : `500` avec message texte (fichier vide, shard non transmissible
après retries, registre injoignable…).

## `GET /f/{hash}`

Télécharge le fichier, reconstruit en **streaming** (une stripe en mémoire
à la fois) depuis l'ensemble du cluster : shards locaux d'abord, puis
demandés aux autres membres. k shards valides par stripe suffisent — nœuds
morts et shards corrompus sont compensés par Reed-Solomon, de façon
invisible pour le client.

- `Content-Length` : taille exacte du fichier.
- `Content-Disposition: attachment; filename="<name>"` si un nom a été
  fourni à l'upload.
- Intégrité : hash global recalculé pendant le stream ; un pair injoignable
  est mémorisé par requête (timeout connexion 3 s, transfert 20 s) et
  n'est pas recontacté à chaque shard.
- `404` si le hash est inconnu du registre.

```
curl -o video.mp4 http://node3:8080/f/988f6e61…
```

## `GET /api/files`

Le registre répliqué (état local du nœud, éventuellement en retard de
quelques centaines de ms sur le leader) :

```json
[
  { "hash": "988f6e61…", "size": 30000000,
    "name": "video.mp4", "link": "/f/988f6e61…" }
]
```

## Ce qui n'existe pas encore (v1)

- `DELETE` / expiration des fichiers (l'`UnregisterManifest` existe côté
  Raft ; il manque le nettoyage des shards orphelins).
- Authentification, quotas, rate-limiting.
- Uploads multipart / reprise d'upload interrompu.

## Interface web

Chaque nœud sert la webui (si `webui/dist` existe, ou `--webui <dir>`) :
pages Fichiers (upload chiffré drag & drop, trousseau local de clés,
liens de partage), Cluster (statut live via `GET /api/status`), et
`/d/{hash}#clé` (téléchargement + déchiffrement dans le navigateur).

L'interface est dérivée de la webui de **ZeroFS**
(https://github.com/Barre/ZeroFS, AGPL-3.0) — voir `webui/ATTRIBUTION.md`.
Le chiffrement navigateur (WebCrypto AES-256-GCM) est compatible bit à bit
avec `yog-crypto` : un fichier uploadé par la CLI se déchiffre dans le
navigateur et réciproquement.

Construire : `cd webui && npm install && npm run build`.

### Requêtes partielles (Range)

`GET /f/{hash}` accepte `Range: bytes=…` et répond `206 Partial Content`
avec `Content-Range` (`416` si la plage est hors fichier ; `Accept-Ranges:
bytes` annoncé partout, y compris en `HEAD`). Seules les stripes qui
intersectent la plage sont récupérées du cluster et décodées — lire 64
octets au milieu d'un fichier de 81 Mo ne coûte qu'un aller-retour
(mesuré : ~400 ms sur un cluster local, plutôt que le fichier entier).

Sert à la reprise de téléchargement et à la lecture média.

### Lecteur média chiffré (`/w/{hash}#clé`)

**Mode nominal — streaming.** Un Service Worker sert `/stream/{hash}` en
clair à partir du ciphertext : pour chaque plage demandée par `<video>`,
seuls les chunks AES-GCM concernés sont tirés du cluster (Range sur le
ciphertext), déchiffrés et rendus. **Rien n'est chargé d'avance** — la
lecture démarre immédiatement et un seek ne coûte qu'un aller-retour,
quelle que soit la taille du fichier. La clé est transmise au worker par
IndexedDB, jamais par le réseau.

Deux pièges rencontrés et corrigés, utiles à connaître avant de toucher
`webui/public/sw-stream.js` :

- l'état mémoire d'un Service Worker est **volatile** (le navigateur
  l'arrête entre deux événements) — d'où IndexedDB plutôt qu'une `Map` ;
- un worker qui streame une réponse pendant des dizaines de secondes est
  **tué** (le lecteur reçoit un 503) — d'où des réponses bornées à 4 Mio,
  renvoyées en 206, que le lecteur enchaîne.

**Repli.** Si la lecture n'a pas démarré au bout de 6 s (worker
indisponible, navigateur restrictif), le lecteur bascule silencieusement
sur un déchiffrement complet en mémoire + Blob URL : robuste, mais il
faut attendre le fichier entier, donc plafonné à 600 Mo. Un badge
« streaming » dans l'interface indique quel mode est actif.

## Suppression, expiration et bannissement

### `DELETE /f/{hash}`
Retire le fichier du registre répliqué (`204 No Content`, `404` s'il est
inconnu). Chaque nœud purge ensuite ses manifests et shards devenus
orphelins à la passe de fond suivante. Mesuré : 6/6/6 shards → 0/0/0 en un
cycle sur un cluster de 3.

### TTL — `POST /api/upload?ttl=<secondes>`
Le manifest porte un `expires_at`. Le **leader** retire les fichiers échus
du registre (une fois pour tout le cluster), la purge suit partout. Les
fichiers expirés disparaissent du listing et ne sont plus servis.

### Bannissement — `yog-node ban <hash> --reason "…"`
Pour honorer un signalement ou une réquisition **sans jamais lire le
contenu** : le hash est banni dans l'état Raft, le fichier sort du
registre, `GET` répond **`410 Gone` avec le motif**, les shards sont purgés,
et tout **ré-upload du même contenu est refusé** (le registre rejette le
manifest). `yog-node unban <hash>` lève la mesure.

Limite structurelle assumée : le bannissement ne vise que ce contenu à
l'octet près — un ré-upload chiffré avec une autre clé produit un autre
hash. Voir [chiffrement.md](chiffrement.md#réquisition-judiciaire--ce-que-lopérateur-peut-fournir).

### Sécurité de la purge
Un nœud ne purge **que** si son registre est fiable (membre du cluster et
leader connu) : un nœud fraîchement démarré, au registre encore vide,
n'efface rien — sinon il détruirait le cluster. Un shard référencé par un
autre fichier vivant n'est jamais supprimé (testé).
