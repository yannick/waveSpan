#!/usr/bin/env bash
# Publish the built static documentation site (./docs-site) to the `gh-pages` branch of the remote,
# which GitHub Pages serves at https://<owner>.github.io/<repo>/.
#
#   make docs-site      # build ./docs-site   (or: just docs-site)
#   make docs-deploy    # build + run this script   (or: just docs-deploy)
#
# The site is a generated artifact, so this publishes a single-commit ORPHAN history and force-pushes
# each time — gh-pages never accumulates old builds, and nothing in the main branch is touched.
#
# Env overrides:
#   PAGES_BRANCH   target branch (default: gh-pages)
#   PAGES_REMOTE   push target   (default: the `origin` remote URL) — handy for testing/forks
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SITE="$ROOT/docs-site"
BRANCH="${PAGES_BRANCH:-gh-pages}"
REMOTE_URL="${PAGES_REMOTE:-$(git -C "$ROOT" remote get-url origin)}"

if [ ! -f "$SITE/index.html" ]; then
  echo "error: $SITE/index.html not found — run 'make docs-site' (or 'just docs-site') first." >&2
  exit 1
fi

# .nojekyll: serve Vite's output verbatim (skip GitHub Pages' Jekyll processing).
touch "$SITE/.nojekyll"

WT="$(mktemp -d)"
trap 'rm -rf "$WT"' EXIT

NAME="$(git -C "$ROOT" config user.name || echo 'docs deploy')"
EMAIL="$(git -C "$ROOT" config user.email || echo 'docs-deploy@localhost')"

git -C "$WT" init -q
git -C "$WT" checkout -q --orphan "$BRANCH"
cp -R "$SITE"/. "$WT"/                       # contents only (incl. .nojekyll), not the dir itself
git -C "$WT" add -A
git -C "$WT" -c user.name="$NAME" -c user.email="$EMAIL" commit -qm "Deploy documentation site"

echo "Pushing ./docs-site → $BRANCH on $REMOTE_URL …"
git -C "$WT" push -f "$REMOTE_URL" "HEAD:$BRANCH"

# Friendly Pages URL hint for GitHub remotes.
case "$REMOTE_URL" in
  *github.com*)
    slug="$(printf '%s' "$REMOTE_URL" | sed -E 's#(git@github.com:|https://github.com/)##; s#\.git$##')"
    owner="${slug%%/*}"; repo="${slug##*/}"
    echo "✓ Pushed. One-time setup: GitHub → Settings → Pages → Branch: $BRANCH / (root)."
    echo "  Site: https://${owner}.github.io/${repo}/"
    ;;
  *)
    echo "✓ Pushed to $BRANCH on $REMOTE_URL."
    ;;
esac
