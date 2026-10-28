#!/bin/sh
# cf-retire.sh: list cf_* properties whose commitfest entry is closed.
#
# Each cf_* property names its entry as "CF NNNN" in its comment.  For each
# entry this asks the commitfest app for its status; an entry that is
# Committed, Withdrawn, Rejected or Returned with feedback is printed with
# the files that mention it.  A committed entry's property should be deleted
# (the patch's own regression test now covers it); for the others, decide
# whether the bug is still open on master (keep) or the property is moot.
#
# Usage: src/test/prop/scripts/cf-retire.sh   (needs curl and network)
set -eu
dir=$(cd "$(dirname "$0")/../sql" && pwd)
ids=$(grep -ohE 'CF [0-9]{4}' "$dir"/prop_cf_*.c "$dir"/prop_syscol.c 2>/dev/null |
	awk '{print $2}' | sort -u)
for cf in $ids; do
	st=$(curl -s -m 20 "https://commitfest.postgresql.org/patch/$cf/" |
		sed 's/<[^>]*>/\n/g' | grep -v '^[[:space:]]*$' |
		sed -n '/^Status$/,+3p' | sed -n 4p | sed 's/^ *//')
	case "$st" in
		Committed|Withdrawn|Rejected|"Returned with feedback")
			printf '%s\t%s\t%s\n' "$cf" "$st" \
				"$(grep -l "CF $cf" "$dir"/prop_cf_*.c "$dir"/prop_syscol.c | xargs -n1 basename | tr '\n' ' ')"
			;;
		"") printf '%s\t(status unavailable)\n' "$cf" >&2 ;;
	esac
done
