#!/bin/bash

usage() {
	echo "$0 [sqlite-amalgamation-url]"
	echo
	echo "Lookup the URL to a SQLite amalgamation zip on https://sqlite.org"
	echo "Pass that URL to this tool, e.g."
	echo "  $0 https://sqlite.org/2024/sqlite-amalgamation-3460100.zip"
}

fatal() {
	echo "$@" >&2
	exit 1
}

case "$1" in
	https://sqlite.org/*) ;;
	-h|--help|help)
		usage
		exit
		;;
	*)
		usage >&2
		exit 1
		;;
esac

cd "$( dirname "${BASH_SOURCE[0]}" )"/cgosqlite || fatal "Not in correct directory"

url="$1"
filename=$(basename "$url")
dirname=$(basename -s .zip "$filename")
[[ -n "$filename" ]] || fatal "Could not extract filename from $url"
[[ -n "$dirname" ]] || fatal "Could not extract dirname from $filename"

trap "rm -rf ./${filename} ./${dirname}" EXIT

curl -O "$1" || fatal "Download of $url failed"
[[ -f "$filename" ]] || fatal "File $filename not found after download"
unzip "$filename" || fatal "Unzip of $filename failed"
[[ -d "$dirname" ]] || fatal "Directory $dirname missing after unzip"
mv "$dirname"/*.{c,h} .
mv shell.c shell.c.disabled

