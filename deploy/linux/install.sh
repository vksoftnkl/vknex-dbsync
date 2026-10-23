#!/bin/sh
# Sets up vknex-dbsync for the current Linux desktop user: builds the binary,
# adds a systemd user service that runs it in the background (settings from
# .env, logs appended to vknex-dbsync.log) and a "VK Nex DB Sync" icon in the
# app grid and on the desktop. Run it again after moving the project folder.
#
#   install.sh [ssh-host]
#
# With an ssh host (e.g. erp-vps) it also adds vknex-dbsync-tunnel.service,
# which keeps "ssh -N <host>" open while the sync runs and reconnects when it
# drops. The host's LocalForward in ~/.ssh/config must forward the port that
# SYNC_TARGET_URL uses, and its key must work without a passphrase prompt.
set -eu

repo=$(cd "$(dirname "$0")/../.." && pwd)
unit="$HOME/.config/systemd/user/vknex-dbsync.service"
tunnel_unit="$HOME/.config/systemd/user/vknex-dbsync-tunnel.service"
app="$HOME/.local/share/applications/vknex-dbsync.desktop"
tunnel_host=${1:-}

cd "$repo"
go build -o vknex-dbsync ./cmd/vknex-dbsync
chmod +x deploy/linux/control.sh
[ -f .env ] || echo "warning: $repo/.env is missing; copy .env.example and fill it in" >&2

mkdir -p "$(dirname "$unit")" "$(dirname "$app")"

deps=
if [ -n "$tunnel_host" ]; then
	ssh -o BatchMode=yes -o ConnectTimeout=10 -o ClearAllForwardings=yes "$tunnel_host" true ||
		echo "warning: ssh $tunnel_host failed; the tunnel service will keep retrying" >&2
	deps="Wants=vknex-dbsync-tunnel.service
After=vknex-dbsync-tunnel.service"
	cat >"$tunnel_unit" <<EOF
[Unit]
Description=SSH tunnel to the live database for vknex-dbsync
# stopped and restarted together with the sync
PartOf=vknex-dbsync.service

[Service]
# -f forks only once the forwarded port listens, so the sync (After=) never starts before it
Type=forking
ExecStart=$(command -v ssh) -f -N -o BatchMode=yes -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=3 $tunnel_host
Restart=always
RestartSec=10
EOF
else
	systemctl --user stop vknex-dbsync-tunnel.service 2>/dev/null || true
	rm -f "$tunnel_unit"
fi

cat >"$unit" <<EOF
[Unit]
Description=VK Nex local -> live DB sync
$deps

[Service]
WorkingDirectory=$repo
ExecStart="$repo/vknex-dbsync"
StandardOutput=append:$repo/vknex-dbsync.log
StandardError=inherit
Restart=on-failure
RestartSec=30
# 2 is a configuration error: retrying will not help until .env is fixed
RestartPreventExitStatus=2

[Install]
WantedBy=default.target
EOF

cat >"$app" <<EOF
[Desktop Entry]
Type=Application
Name=VK Nex DB Sync
Comment=Sync the local database to the live database in the background
Icon=emblem-synchronizing
Exec="$repo/deploy/linux/control.sh" start
Terminal=false
Actions=stop;log;

[Desktop Action stop]
Name=Stop
Exec="$repo/deploy/linux/control.sh" stop

[Desktop Action log]
Name=Show log
Exec="$repo/deploy/linux/control.sh" log
EOF

systemctl --user daemon-reload

if [ -d "$HOME/Desktop" ]; then
	cp "$app" "$HOME/Desktop/vknex-dbsync.desktop"
	chmod +x "$HOME/Desktop/vknex-dbsync.desktop"
	gio set "$HOME/Desktop/vknex-dbsync.desktop" metadata::trusted true 2>/dev/null || true
fi

echo "Installed. Click \"VK Nex DB Sync\" on the desktop or in the app grid to start it."
[ -z "$tunnel_host" ] || echo "Starting it also opens the SSH tunnel to $tunnel_host (vknex-dbsync-tunnel.service)."
echo "To also start it at every login: systemctl --user enable vknex-dbsync.service"
