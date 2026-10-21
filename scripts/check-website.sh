#!/usr/bin/env sh
# Verify the committed docs site is exactly what website/build.py generates
# (CHANGELOG.md -> website/src/pages/changelog.html -> website/public/).
#
# Used by CI ("docs site builds") and by scripts/git-hooks/pre-commit, so the
# local check and the CI check can never disagree. Exits non-zero, listing
# the stale or untracked generated files, when the site needs rebuilding.
set -eu
cd "$(dirname "$0")/.."
python3 website/build.py >/dev/null
# Only what the rebuild changed relative to the index counts: an unstaged
# modification (second status column) or an untracked generated file. Staged
# changes are fine — inside the pre-commit hook they *are* the fix — and a
# clean CI checkout has none, so CI's check is unchanged.
stale=$(git status --porcelain -- website/public website/src/pages/changelog.html \
    | grep -E '^(.[^ ]|\?\?)' || true)
if [ -n "$stale" ]; then
    echo "error: the docs site is stale or has uncommitted generated files:" >&2
    echo "$stale" | sed 's/^/  /' >&2
    echo "website/build.py has regenerated them; review, then:" >&2
    echo "  git add website/public website/src/pages/changelog.html" >&2
    exit 1
fi
echo "docs site is up to date"
