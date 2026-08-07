# Attribution

This web interface is derived from the **ZeroFS webui**
(https://github.com/Barre/ZeroFS), created by Pierre Barre and the ZeroFS
contributors, under the **AGPL-3.0** license (see `LICENSE` in this
directory — the license applies to everything under `webui/`).

Thanks to ZeroFS for this remarkable work. ❤️

## Changes made here

- The ZeroFS data access layer (ConnectRPC/gRPC, `lib/zerofs`, `lib/grpc`)
  is replaced by an HTTP client for the yogfile API (`lib/yog`), with
  WebCrypto end-to-end encryption (AES-256-GCM, format compatible with the
  `nauka-crypto` crate).
- The pages tied to filesystem semantics (tree-based file manager,
  terminal/v86 VM) are removed; the files and monitoring pages are rewritten
  for a flat content-addressed store.
- The design system (`ui/` components, layout, Tailwind styles) and the
  application structure are kept from ZeroFS.

The full source of this interface, modifications included, is distributed in
this repository in accordance with the AGPL-3.0.
