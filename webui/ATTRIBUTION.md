# Attribution

Cette interface web est dérivée de la **webui de ZeroFS**
(https://github.com/Barre/ZeroFS), créée par Pierre Barre et les
contributeurs de ZeroFS, sous licence **AGPL-3.0** (voir `LICENSE` dans ce
répertoire — la licence s'applique à tout le contenu de `webui/`).

Merci à ZeroFS pour ce travail remarquable. ❤️

## Modifications apportées ici

- La couche d'accès aux données ZeroFS (ConnectRPC/gRPC, `lib/zerofs`,
  `lib/grpc`) est remplacée par un client HTTP vers l'API yogfile
  (`lib/yog`), avec chiffrement de bout en bout WebCrypto (AES-256-GCM,
  format compatible avec la crate `yog-crypto`).
- Les pages liées aux sémantiques de système de fichiers (gestionnaire
  arborescent, terminal/VM v86) sont retirées ; les pages fichiers et
  monitoring sont réécrites pour un store plat content-addressed.
- Le design system (composants `ui/`, layout, styles Tailwind) et la
  structure de l'application sont conservés de ZeroFS.

Le source complet de cette interface, modifications comprises, est
distribué dans ce dépôt conformément à l'AGPL-3.0.
