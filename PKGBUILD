pkgname=c2a
pkgver=0.1.0
pkgrel=1
pkgdesc='Local Responses API relay for a ChatGPT Codex subscription'
arch=('x86_64')
url='https://github.com/ylxdzsw/c2a'
license=('LicenseRef-Unknown')
depends=('gcc-libs' 'glibc' 'openssl')
makedepends=('cargo' 'git' 'pkgconf')
options=('!lto')
_source_url="${C2A_SOURCE_URL:-$url.git}"
_source_branch="${C2A_SOURCE_BRANCH:-master}"
source=("$pkgname::git+$_source_url#branch=$_source_branch")
sha256sums=('SKIP')

pkgver() {
  cd "$pkgname"
  local _ver="$(grep -Po '^version\s*=\s*"\K[^"]*' Cargo.toml)"
  printf '%s.r%s.g%s' "$_ver" "$(git rev-list --count HEAD)" "$(git rev-parse --short HEAD)"
}

build() {
  export CARGO_HOME="$srcdir/cargo-home"
  export CARGO_TARGET_DIR="$srcdir/target"
  export CARGO_INCREMENTAL=0
  cargo build --manifest-path "$srcdir/$pkgname/Cargo.toml" --release --locked
}

check() {
  export CARGO_HOME="$srcdir/cargo-home"
  export CARGO_TARGET_DIR="$srcdir/target"
  cargo test --manifest-path "$srcdir/$pkgname/Cargo.toml" --locked --all-targets
}

package() {
  install -Dm755 "$srcdir/target/release/c2a" "$pkgdir/usr/bin/c2a"
  install -Dm644 "$srcdir/$pkgname/c2a.service" \
    "$pkgdir/usr/lib/systemd/system/c2a.service"
  install -Dm644 "$srcdir/$pkgname/c2a.socket" \
    "$pkgdir/usr/lib/systemd/system/c2a.socket"
  install -Dm644 "$srcdir/$pkgname/README.md" \
    "$pkgdir/usr/share/doc/$pkgname/README.md"
}
