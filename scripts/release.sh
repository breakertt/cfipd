#!/usr/bin/env bash
set -euo pipefail
version=${1:?usage: release.sh VERSION [OUTPUT_DIR]}
root=$(cd "$(dirname "$0")/.." && pwd)
output=${2:-$root/dist}
mkdir -p "$output"
output=$(cd "$output" && pwd)
deb_version=${version/-/~}
dpkg --validate-version "$deb_version"
commit=$(git -C "$root" rev-parse HEAD)
export SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH:-$(git -C "$root" show -s --format=%ct HEAD)}
build_date=$(date -u -d "@$SOURCE_DATE_EPOCH" +%Y-%m-%dT%H:%M:%SZ)
mkdir -p "$output/.packages"
cd "$root"
while read -r label goarch debarch goarm; do
    binary="$output/cfipd_${version}_linux_${label}"
    CGO_ENABLED=0 GOOS=linux GOARCH="$goarch" GOARM="$goarm" GOAMD64=v1 \
        go build -trimpath -ldflags "-s -w -X main.version=v$version -X main.commit=$commit -X main.buildDate=$build_date" -o "$binary" .
    stage=$(mktemp -d "$output/.packages/${debarch}.XXXXXX")
    install -Dm755 "$binary" "$stage/usr/bin/cfipd"
    install -Dm644 packaging/cfipd.json "$stage/etc/cfipd.json"
    for unit in cfipd cfipd-web; do
        install -Dm644 "packaging/$unit.service" "$stage/usr/lib/systemd/system/$unit.service"
    done
    for readme in README.md README.zh.md; do
        install -Dm644 "$readme" "$stage/usr/share/doc/cfipd/$readme"
    done
    mkdir -p "$stage/DEBIAN"
    for script in postinst prerm postrm; do
        install -m755 "packaging/$script" "$stage/DEBIAN/$script"
    done
    printf '/etc/cfipd.json\n' > "$stage/DEBIAN/conffiles"
    cat > "$stage/DEBIAN/control" <<EOF
Package: cfipd
Version: $deb_version
Architecture: $debarch
Maintainer: Tianyi Gao <tianyi.gao@ed.ac.uk>
Section: net
Priority: optional
Installed-Size: $(du -sk "$stage" | cut -f1)
Depends: ca-certificates, adduser, init-system-helpers (>= 1.54)
Homepage: https://github.com/breakertt/cfipd
Description: Cloudflare HTTPS relay with adaptive IP selection
 Maintains a small pool of tested Cloudflare endpoints, with adaptive
 probing, an HTTPS relay with TLS passthrough, and a read-only dashboard.
EOF
    dpkg-deb --root-owner-group -Zxz --build "$stage" "$output/cfipd_${version}_${debarch}.deb"
done <<'TARGETS'
amd64 amd64 amd64 7
arm64 arm64 arm64 7
armv7 arm armhf 7
386 386 i386 7
riscv64 riscv64 riscv64 7
TARGETS
cd "$output"
sha256sum cfipd_"$version"_* > SHA256SUMS
printf 'Release assets: %s\nPackage staging: %s/.packages\n' "$output" "$output"
