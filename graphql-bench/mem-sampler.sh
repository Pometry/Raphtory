#!/usr/bin/env bash
# Prints one line of memory usage every INTERVAL seconds until killed, e.g.
#   12:01:05 used=9120M avail=6210M swap=0M server_rss=7012M server_threads=41
# so a server crash can be told apart from one under memory pressure: with pressure, `avail`
# heads to zero (and swap fills) in the lines before the crash.
#
#   mem-sampler.sh [INTERVAL] > mem.log &
INTERVAL=${1:-5}
while true; do
	read -r used avail < <(free -m | awk '/^Mem:/ {print $3, $7}')
	swap=$(free -m | awk '/^Swap:/ {print $3}')
	# the python process itself, not the `sh -c` (or debugger) it was started from
	pid=$(for p in $(pgrep -f 'server\.py'); do
		case $(cat "/proc/$p/comm" 2>/dev/null) in python*) echo "$p" ;; esac
	done | head -n 1)
	if [ -n "$pid" ]; then
		rss=$(awk '/^VmRSS:/ {print int($2 / 1024) "M"}' "/proc/$pid/status" 2>/dev/null)
		threads=$(awk '/^Threads:/ {print $2}' "/proc/$pid/status" 2>/dev/null)
	else
		rss=- threads=-
	fi
	echo "$(date -u +%T) used=${used}M avail=${avail}M swap=${swap}M server_rss=${rss:--} server_threads=${threads:--}"
	sleep "$INTERVAL"
done
