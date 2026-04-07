#!/bin/sh
# Install script for nauka — downloads the latest release binary.
set -eu

REPO="sifrah/nauka"
BIN="nauka"
INSTALL_DIR="/usr/local/bin"
CHANNEL="stable"

# Parse arguments
for arg in "$@"; do
  case "$arg" in
    --nightly) CHANNEL="nightly" ;;
    --beta)    CHANNEL="beta" ;;
    --stable)  CHANNEL="stable" ;;
  esac
done

DOCS_URL="https://sifrah.github.io/nauka/${CHANNEL}/"

# --- UX helpers -----------------------------------------------------------

CHECK="\342\234\223"   # ✓
CROSS="\342\234\227"   # ✗

step_ok()   { printf "  %b %s\n" "$CHECK" "$1"; }
step_fail() { printf "  %b %s\n" "$CROSS" "$1" >&2; }

SPINNER_CHARS='|/-\'
SPINNER_PID=""

start_spinner() {
  _msg="$1"
  (
    i=0
    while true; do
      c=$(printf '%s' "$SPINNER_CHARS" | cut -c$(( (i % 4) + 1 )))
      printf "\r  %s %s" "$c" "$_msg"
      i=$(( i + 1 ))
      sleep 0.15 2>/dev/null || sleep 1
    done
  ) &
  SPINNER_PID=$!
}

stop_spinner() {
  _final_msg="$1"
  _status="${2:-ok}"
  if [ -n "$SPINNER_PID" ]; then
    kill "$SPINNER_PID" 2>/dev/null || true
    wait "$SPINNER_PID" 2>/dev/null || true
    SPINNER_PID=""
  fi
  printf "\r                                                                \r"
  if [ "$_status" = "ok" ]; then
    step_ok "$_final_msg"
  else
    step_fail "$_final_msg"
  fi
}

cleanup() {
  if [ -n "$SPINNER_PID" ]; then
    kill "$SPINNER_PID" 2>/dev/null || true
    wait "$SPINNER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

# --- Detect platform -------------------------------------------------------

OS="$(uname -s)"
case "$OS" in
  Linux)  OS="unknown-linux-musl" ;;
  Darwin) OS="apple-darwin" ;;
  *)
    step_fail "Unsupported operating system: $OS"
    exit 1
    ;;
esac

ARCH="$(uname -m)"
case "$ARCH" in
  x86_64)        ARCH="x86_64" ;;
  aarch64|arm64) ARCH="aarch64" ;;
  *)
    step_fail "Unsupported architecture: $ARCH"
    exit 1
    ;;
esac

TARGET="${ARCH}-${OS}"

# --- Fetch release ----------------------------------------------------------

if [ "$CHANNEL" = "stable" ]; then
  start_spinner "Fetching latest stable release..."
  VERSION="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
    | grep '"tag_name"' | head -1 | sed 's/.*"tag_name": *"//;s/".*//')" || true
  if [ -z "$VERSION" ]; then
    stop_spinner "Could not determine latest release version" fail
    exit 1
  fi
  stop_spinner "Latest version: ${VERSION}"
else
  start_spinner "Fetching latest ${CHANNEL} release..."
  VERSION="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases?per_page=30" \
    | grep '"tag_name"' | grep "${CHANNEL}" \
    | sed 's/.*"tag_name": *"//;s/".*//' | sort -V | tail -1)" || true
  if [ -z "$VERSION" ]; then
    stop_spinner "No ${CHANNEL} release found" fail
    exit 1
  fi
  stop_spinner "Latest ${CHANNEL}: ${VERSION}"
fi

# --- Download ---------------------------------------------------------------

ARCHIVE="${BIN}-${VERSION}-${TARGET}.tar.gz"
URL="https://github.com/${REPO}/releases/download/${VERSION}/${ARCHIVE}"

TMPDIR="$(mktemp -d)"
trap 'cleanup; rm -rf "$TMPDIR"' EXIT

start_spinner "Downloading ${BIN} ${VERSION}..."
if curl -fsSL -o "${TMPDIR}/${ARCHIVE}" "$URL"; then
  SIZE=$(wc -c < "${TMPDIR}/${ARCHIVE}" | tr -d ' ')
  SIZE_MB=$(( SIZE / 1048576 ))
  if [ "$SIZE_MB" -gt 0 ]; then
    stop_spinner "Downloaded ${BIN} ${VERSION} (${SIZE_MB} MB)"
  else
    SIZE_KB=$(( SIZE / 1024 ))
    stop_spinner "Downloaded ${BIN} ${VERSION} (${SIZE_KB} KB)"
  fi
else
  stop_spinner "Failed to download ${URL}" fail
  exit 1
fi

# --- Verify checksum --------------------------------------------------------

CHECKSUMS_URL="https://github.com/${REPO}/releases/download/${VERSION}/SHA256SUMS.txt"
start_spinner "Verifying checksum..."

if ! curl -fsSL -o "${TMPDIR}/SHA256SUMS.txt" "$CHECKSUMS_URL"; then
  stop_spinner "Could not download SHA256SUMS.txt — verify the release manually" fail
  exit 1
fi

EXPECTED="$(grep -F "${ARCHIVE}" "${TMPDIR}/SHA256SUMS.txt" | head -1 | awk '{print $1}')"
if [ -z "$EXPECTED" ]; then
  stop_spinner "No checksum found for ${ARCHIVE}" fail
  exit 1
fi

if command -v sha256sum > /dev/null 2>&1; then
  ACTUAL="$(sha256sum "${TMPDIR}/${ARCHIVE}" | awk '{print $1}')"
elif command -v shasum > /dev/null 2>&1; then
  ACTUAL="$(shasum -a 256 "${TMPDIR}/${ARCHIVE}" | awk '{print $1}')"
else
  stop_spinner "No sha256sum or shasum found" fail
  exit 1
fi

if [ "$EXPECTED" != "$ACTUAL" ]; then
  stop_spinner "Checksum mismatch" fail
  printf "    expected: %s\n" "$EXPECTED" >&2
  printf "    actual:   %s\n" "$ACTUAL" >&2
  exit 1
fi

stop_spinner "Checksum verified"

# --- Install ----------------------------------------------------------------

start_spinner "Extracting..."
if tar xzf "${TMPDIR}/${ARCHIVE}" -C "$TMPDIR"; then
  stop_spinner "Extracted"
else
  stop_spinner "Failed to extract ${ARCHIVE}" fail
  exit 1
fi

start_spinner "Installing to ${INSTALL_DIR}/${BIN}..."
if install -m 755 "${TMPDIR}/${BIN}" "${INSTALL_DIR}/${BIN}"; then
  stop_spinner "Installed to ${INSTALL_DIR}/${BIN}"
else
  stop_spinner "Failed — are you root?" fail
  exit 1
fi

# --- Verify -----------------------------------------------------------------

EXPECTED_VERSION="${VERSION#v}"

if command -v "$BIN" > /dev/null 2>&1; then
  ACTUAL_VERSION=$("$BIN" --version 2>/dev/null | awk '{print $2}') || true
  if [ -n "$ACTUAL_VERSION" ] && [ "$ACTUAL_VERSION" = "$EXPECTED_VERSION" ]; then
    step_ok "Verified: ${BIN} ${ACTUAL_VERSION}"
  fi
else
  step_fail "${BIN} installed to ${INSTALL_DIR} but is not on PATH"
  exit 1
fi

# --- Done -------------------------------------------------------------------

printf "\n%s v%s installed successfully.\n" "$BIN" "$EXPECTED_VERSION"
printf "Docs: %s\n" "$DOCS_URL"
