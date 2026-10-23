#!/bin/sh
# Sets up vknex-dbsync for the current Linux desktop user: builds the binary,
# adds a systemd user service that runs it in the background (settings from
# .env, logs appended to vknex-dbsync.log) and a "VK Nex DB Sync" icon in the
# app grid and on the desktop. Run it again after moving the project folder.
set -eu

repo=$(cd "$(dirname "$0")/../.." && pwd)
unit="$HOME/.config/systemd/user/vknex-dbsync.service"
app="$HOME/.local/share/applications/vknex-dbsync.desktop"

cd "$repo"
go build -o vknex-dbsync ./cmd/vknex-dbsync
chmod +x deploy/linux/control.sh
[ -f .env ] || echo "warning: $repo/.env is missing; copy .env.example and fill it in" >&2

mkdir -p "$(dirname "$unit")" "$(dirname "$app")"
cat >"$unit" <<EOF
[Unit]
Description=VK Nex local -> live DB sync

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
echo "To also start it at every login: systemctl --user enable vknex-dbsync.service"
