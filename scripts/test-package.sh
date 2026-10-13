#!/usr/bin/env bash
# Runs on a disposable systemd host, never on the production relay machine.
set -euo pipefail
package=$(realpath "${1:?usage: test-package.sh PACKAGE.deb}")
if [[ -e /etc/cfipd.json || -e /var/lib/cfipd/state.json ]]; then
    printf 'Refusing package lifecycle tests on an existing cfipd installation.\n' >&2
    exit 1
fi
# Test installation without consuming external probe/download traffic.
sudo install -d /etc/systemd/system/cfipd.service.d
printf '[Service]\nPrivateNetwork=yes\n' | sudo tee /etc/systemd/system/cfipd.service.d/ci.conf >/dev/null
wait_ctl() {
    for attempt in {1..20}; do
        if sudo cfipd status 2>/dev/null; then return; fi
        sleep 0.25
    done
    return 1
}
sudo apt-get install -y "$package"
systemctl is-enabled --quiet cfipd
systemctl is-active --quiet cfipd
test "$(systemctl show cfipd -p User --value)" = cfipd
test "$(stat -c %U /var/lib/cfipd)" = cfipd
sudo cfipd version
wait_ctl
sudo systemctl enable --now cfipd-web
curl --retry 10 --retry-connrefused --retry-delay 1 -fsS http://127.0.0.1:8080/api/status
# Upgrading must preserve administrator configuration and disabled services.
sudo python3 - <<'PY'
import json,pathlib
p=pathlib.Path('/etc/cfipd.json'); data=json.loads(p.read_text())
data['scan_v6_ratio']=0
p.write_text(json.dumps(data,indent=2)+'\n')
PY
config_hash=$(sha256sum /etc/cfipd.json)
sudo systemctl disable --now cfipd-web cfipd
sudo dpkg -i "$package"
test "$(sha256sum /etc/cfipd.json)" = "$config_hash"
for unit in cfipd cfipd-web; do
    test "$(systemctl is-enabled "$unit")" = disabled
    ! systemctl is-active --quiet "$unit"
done
sudo systemctl enable --now cfipd
wait_ctl
sudo dpkg --remove cfipd
! systemctl is-active --quiet cfipd
test -f /etc/cfipd.json
sudo test -f /var/lib/cfipd/state.json
printf '\nPackage install, service startup, configuration preservation and removal passed.\n'
