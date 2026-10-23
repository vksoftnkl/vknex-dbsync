#!/bin/sh
# Used by the "VK Nex DB Sync" desktop icon (see install.sh):
#   control.sh open    start the sync if it is not running, then open its status page in the browser
#   control.sh start   run the sync in the background as a systemd user service
#   control.sh stop    stop it
#   control.sh log     follow the log in a terminal
unit=vknex-dbsync.service
title="VK Nex DB Sync"
repo=$(cd "$(dirname "$0")/../.." && pwd)
log="$repo/vknex-dbsync.log"

# The status page address: SYNC_UI in .env (an empty value turns the page
# off), else the binary's default.
ui_line=$(grep -E '^[[:space:]]*(export[[:space:]]+)?SYNC_UI=' "$repo/.env" 2>/dev/null | tail -n 1)
if [ -n "$ui_line" ]; then
	ui=$(printf '%s' "$ui_line" | sed 's/^[^=]*=//; s/[[:space:]"'"'"']//g')
else
	ui=127.0.0.1:7645
fi
case "$ui" in
'') url= ;;
:*) url="http://127.0.0.1$ui/" ;;
0.0.0.0:*) url="http://127.0.0.1${ui#0.0.0.0}/" ;;
*) url="http://$ui/" ;;
esac

# running: the service is active.
running() { systemctl --user is-active --quiet "$unit"; }

# start_service starts the sync and reports whether it is still running 3 s
# later: a bad .env makes it exit at once.
start_service() {
	running && return 0
	systemctl --user start "$unit"
	sleep 3
	running
}

# answers: the status page replies.
answers() {
	if command -v curl >/dev/null 2>&1; then
		curl -fs -o /dev/null --max-time 2 "${url}api/status"
	else
		wget -q -O /dev/null -T 2 "${url}api/status"
	fi
}

case "${1:-open}" in
open)
	if ! start_service; then
		notify-send -u critical "$title" "Could not start: $(tail -n 1 "$log")"
		exit 1
	fi
	if [ -z "$url" ]; then
		notify-send "$title" "Running in the background. The status page is turned off (SYNC_UI is empty in .env)."
		exit 0
	fi
	# Right after a start the page needs a moment (the tunnel first, then the binary).
	n=0
	until answers; do
		n=$((n + 1))
		if [ "$n" -ge 40 ]; then
			notify-send -u critical "$title" "Running, but the status page at $url does not answer. Check SYNC_UI in .env and the log."
			exit 1
		fi
		sleep 0.5
	done
	xdg-open "$url" >/dev/null 2>&1 &
	;;
start)
	if running; then
		notify-send "$title" "Already running in the background."
	elif start_service; then
		notify-send "$title" "Running in the background."
	else
		notify-send -u critical "$title" "Could not start: $(tail -n 1 "$log")"
	fi
	;;
stop)
	systemctl --user stop "$unit"
	notify-send "$title" "Stopped."
	;;
log)
	gnome-terminal --title "$title log" -- tail -n 100 -f "$log" 2>/dev/null || xdg-open "$log"
	;;
*)
	echo "usage: $0 open|start|stop|log" >&2
	exit 2
	;;
esac
