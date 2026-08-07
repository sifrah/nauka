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
- UI web.
