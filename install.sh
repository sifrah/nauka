#!/bin/sh
# Nauka installer.
#
#   curl -sSfL https://sh.getnauka.com | sh
#
# Environment:
#   VERSION       version to install (default: latest release)
#   INSTALL_DIR   where the binary goes (default: /usr/local/bin)
#   NO_SUDO       set to 1 to never escalate; falls back to ~/.local/bin
#   NAUKA_NO_INIT set to 1 to only install the binary — on a systemd Linux
#                 run as root, the installer otherwise finishes the job:
#                 `nauka init` founds a cluster on this machine, managed by
#                 systemd (started now, back after every reboot)

set -eu

REPO="sifrah/nauka"
BINARY="nauka"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"
VERSION="${VERSION:-latest}"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; DIM='\033[2m'; NC='\033[0m'
info() { printf "${GREEN}==>${NC} %s\n" "$1"; }
warn() { printf "${YELLOW}warning:${NC} %s\n" "$1" >&2; }
die()  { printf "${RED}error:${NC} %s\n" "$1" >&2; exit 1; }

need() { command -v "$1" >/dev/null 2>&1; }

detect_target() {
    os="$(uname -s)"
    arch="$(uname -m)"
    case "$os" in
        Linux)  os_part="unknown-linux-gnu" ;;
        Darwin) os_part="apple-darwin" ;;
        *) die "unsupported operating system: $os (build from source: cargo build --release)" ;;
    esac
    case "$arch" in
        x86_64|amd64)  arch_part="x86_64" ;;
        aarch64|arm64) arch_part="aarch64" ;;
        *) die "no prebuilt binary for $arch (build from source: cargo build --release)" ;;
    esac
    echo "${arch_part}-${os_part}"
}

fetch() {
    # fetch <url> <output>
    if need curl; then
        curl -fsSL "$1" -o "$2"
    elif need wget; then
        wget -qO "$2" "$1"
    else
        die "neither curl nor wget found"
    fi
}

resolve_version() {
    [ "$VERSION" != "latest" ] && { echo "$VERSION"; return; }
    tmp="$(mktemp)"
    # A 404 here means "no release published yet", which is worth telling
    # apart from "the network is down".
    if ! fetch "https://api.github.com/repos/${REPO}/releases/latest" "$tmp" 2>/dev/null; then
        rm -f "$tmp"
        if need curl && curl -fsS -o /dev/null https://api.github.com 2>/dev/null; then
            die "no published release for ${REPO} yet — build from source (cargo build --release) or set VERSION="
        fi
        die "cannot reach GitHub (network or proxy issue); set VERSION= to pin a version"
    fi
    v="$(sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p' "$tmp" | head -1)"
    rm -f "$tmp"
    [ -n "$v" ] || die "no published release yet — build from source, or set VERSION="
    echo "$v"
}

verify_checksum() {
    # verify_checksum <file> <sums-file> <name-in-sums>
    file="$1"; sums="$2"; name="$3"
    expected="$(grep " \{1,2\}\*\?${name}\$" "$sums" 2>/dev/null | awk '{print $1}' | head -1)"
    if [ -z "$expected" ]; then
        warn "no checksum published for ${name}, skipping verification"
        return 0
    fi
    if need sha256sum; then
        actual="$(sha256sum "$file" | awk '{print $1}')"
    elif need shasum; then
        actual="$(shasum -a 256 "$file" | awk '{print $1}')"
    else
        warn "no sha256 tool found, skipping verification"
        return 0
    fi
    [ "$actual" = "$expected" ] || die "checksum mismatch for ${name} — refusing to install"
    info "checksum verified"
}

# Picks a writable destination, escalating only if it has to.
place_binary() {
    src="$1"
    if [ -w "$INSTALL_DIR" ] || mkdir -p "$INSTALL_DIR" 2>/dev/null && [ -w "$INSTALL_DIR" ]; then
        mv "$src" "${INSTALL_DIR}/${BINARY}"
    elif [ "${NO_SUDO:-0}" != "1" ] && need sudo; then
        info "writing to ${INSTALL_DIR} (needs sudo)"
        sudo mkdir -p "$INSTALL_DIR"
        sudo mv "$src" "${INSTALL_DIR}/${BINARY}"
        sudo chmod 755 "${INSTALL_DIR}/${BINARY}"
    else
        INSTALL_DIR="${HOME}/.local/bin"
        warn "cannot write to the system directory, installing to ${INSTALL_DIR}"
        mkdir -p "$INSTALL_DIR"
        mv "$src" "${INSTALL_DIR}/${BINARY}"
    fi
    chmod 755 "${INSTALL_DIR}/${BINARY}" 2>/dev/null || true
}

main() {
    target="$(detect_target)"
    version="$(resolve_version)"
    info "installing ${BINARY} ${version} (${target})"

    tmpdir="$(mktemp -d)"
    trap 'rm -rf "$tmpdir"' EXIT INT TERM

    tarball="nauka-${version#v}-${target}.tar.gz"
    base="https://github.com/${REPO}/releases/download/${version}"

    info "downloading ${tarball}"
    fetch "${base}/${tarball}" "${tmpdir}/${tarball}" \
        || die "download failed — no build for ${target} in ${version}?"

    if fetch "${base}/SHA256SUMS.txt" "${tmpdir}/SHA256SUMS.txt" 2>/dev/null; then
        verify_checksum "${tmpdir}/${tarball}" "${tmpdir}/SHA256SUMS.txt" "$tarball"
    else
        warn "checksums unavailable, skipping verification"
    fi

    tar -xzf "${tmpdir}/${tarball}" -C "$tmpdir"
    found="$(find "$tmpdir" -type f -name "$BINARY" -perm -u+x | head -1)"
    [ -n "$found" ] || die "binary not found inside the archive"

    place_binary "$found"

    printf "\n${GREEN}%s installed${NC} → %s/%s\n" "$BINARY" "$INSTALL_DIR" "$BINARY"
    case ":${PATH}:" in
        *":${INSTALL_DIR}:"*) ;;
        *) warn "${INSTALL_DIR} is not in your PATH — add it to your shell profile" ;;
    esac

    # A machine that already runs the service keeps running it: the new
    # binary is picked up on the next restart, nothing else to redo.
    if [ -f /etc/systemd/system/nauka.service ]; then
        printf "\nThis machine already runs a nauka service. Pick up the new binary with:\n"
        printf "  systemctl restart nauka\n"
        return
    fi

    # On a systemd Linux run as root, finish the job: found a cluster on
    # this machine, systemd-managed — started now, back after every reboot.
    # NAUKA_NO_INIT=1 keeps the installer to the binary alone.
    if [ "$(uname -s)" = "Linux" ] && [ "$(id -u)" = "0" ] \
        && [ -d /run/systemd/system ] && [ "${NAUKA_NO_INIT:-0}" != "1" ]; then
        printf "\n"
        info "founding a cluster on this machine (set NAUKA_NO_INIT=1 to skip)"
        "${INSTALL_DIR}/${BINARY}" init </dev/null
        return
    fi

    printf "\nStart a cluster:\n"
    printf "  ${DIM}# first machine, as root — founds the cluster, systemd-managed${NC}\n"
    printf "  sudo %s init\n\n" "$BINARY"
    printf "  ${DIM}# grow it from that machine — provisions each target over SSH${NC}\n"
    printf "  %s node add <ip>:7311\n\n" "$BINARY"
    printf "Docs: https://getnauka.com\n"
}

main "$@"
