#!/bin/sh
# Used by the "VK Nex DB Sync" desktop icon (see install.sh):
#   control.sh start   run the sync in the background as a systemd user service
#   control.sh stop    stop it
#   control.sh log     follow the log in a terminal
unit=vknex-dbsync.service
title="VK Nex DB Sync"
log="$(cd "$(dirname "$0")/../.." && pwd)/vknex-dbsync.log"

case "${1:-start}" in
start)
	if systemctl --user is-active --quiet "$unit"; then
		notify-send "$title" "Already running in the background."
		exit 0
	fi
	systemctl --user start "$unit"
	sleep 3 # a bad .env makes it exit at once
	if systemctl --user is-active --quiet "$unit"; then
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
	echo "usage: $0 start|stop|log" >&2
	exit 2
	;;
esac
