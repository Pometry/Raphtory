#!/usr/bin/env bash
# Bundles core dumps left by a crashed bench server for offline analysis.
#
#   collect-core.sh [CORE_DIR] [OUT_DIR]     (defaults: /tmp/cores, crash)
#
# For each core in CORE_DIR this
#   - prints the backtrace of every thread (gdb on the machine that crashed sees the exact
#     binaries, so this is the most reliable backtrace you will get), and saves it next to the core
#   - copies every shared object the process had mapped (python, the raphtory extension module,
#     libc, ...) into OUT_DIR/sysroot at their original paths
#   - packs core + sysroot + backtrace into OUT_DIR/<core>.tar.zst
#
# To analyse a bundle elsewhere (CI runs x86_64 Linux; on another arch use gdb-multiarch):
#   mkdir bundle && tar -I zstd -xf core.<...>.tar.zst -C bundle && cd bundle
#   gdb -iex 'set debuginfod enabled off' -ex 'set sysroot sysroot' "sysroot$(cat executable)" core.<...>
#
# Exits 0 whatever happens: it runs after a failure and must not hide the original error.
set -u
CORE_DIR=${1:-/tmp/cores}
OUT=${2:-crash}

shopt -s nullglob
cores=("$CORE_DIR"/core.*)
if [ ${#cores[@]} -eq 0 ]; then
	echo "no core dumps in $CORE_DIR"
	exit 0
fi

if ! command -v gdb >/dev/null 2>&1; then
	echo ">>> installing gdb to read the core dump(s)"
	{ sudo apt-get update -qq && sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq gdb; } >/dev/null 2>&1 \
		|| echo ">>> could not install gdb; bundling the core(s) without a backtrace"
fi

mkdir -p "$OUT"
for core in "${cores[@]}"; do
	name=$(basename "$core")
	work=$(mktemp -d)
	echo ">>> $name ($(du -h "$core" | cut -f1))"

	# the executable the core belongs to, e.g. /usr/bin/python3.14
	exe=$(file -b "$core" 2>/dev/null | sed -n "s/.*execfn: '\([^']*\)'.*/\1/p")
	if [ -n "$exe" ] && [ ! -e "$exe" ]; then
		exe=$(command -v "$exe" || true)
	fi
	exe=${exe:-$(python -c 'import os, sys; print(os.path.realpath(sys.executable))')}
	echo "$exe" >"$work/executable"

	if command -v gdb >/dev/null 2>&1; then
		gdb -q -batch \
			-iex "set debuginfod enabled off" \
			-iex "set pagination off" \
			-ex "echo \n=== crashing thread ===\n" -ex "bt" \
			-ex "echo \n=== faulting instruction ===\n" -ex 'x/i $pc' \
			-ex "echo \n=== fault (signo 11 = SIGSEGV, 7 = SIGBUS, 6 = SIGABRT) ===\n" \
			-ex 'p $_siginfo.si_signo' \
			-ex 'p $_siginfo._sifields._sigfault.si_addr' \
			-ex 'p $sp' \
			-ex "echo sp - fault address within about +-64KiB means a stack overflow (hit the guard page)\n" \
			-ex 'p (long)$sp - (long)$_siginfo._sifields._sigfault.si_addr' \
			-ex "echo \n=== all threads ===\n" -ex "thread apply all bt" \
			-ex "echo \n=== shared libraries ===\n" -ex "info sharedlibrary" \
			"$exe" "$core" >"$work/backtrace.txt" 2>&1
		cat "$work/backtrace.txt"

		# everything the process had mapped, so the bundle opens without this machine
		{
			echo "$exe"
			awk '$1 ~ /^0x/ && $NF ~ /^\// {print $NF}' "$work/backtrace.txt"
		} | sort -u | while read -r lib; do
			[ -f "$lib" ] || continue
			mkdir -p "$work/sysroot$(dirname "$lib")"
			cp -L "$lib" "$work/sysroot$lib"
		done
	else
		cp -L "$exe" "$work/" 2>/dev/null || true
	fi

	mv "$core" "$work/$name"
	if tar -C "$work" -cf - . | zstd -q -T0 -3 -o "$OUT/$name.tar.zst"; then
		cp "$work/backtrace.txt" "$OUT/$name.backtrace.txt" 2>/dev/null || true
		echo ">>> bundled $OUT/$name.tar.zst ($(du -h "$OUT/$name.tar.zst" | cut -f1))"
	else
		echo ">>> failed to bundle $name"
	fi
	rm -rf "$work"
done
exit 0
