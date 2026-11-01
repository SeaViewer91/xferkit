# Releasing

xferkit is distributed from GitHub for now: users install a release tag with

```
pip install "xferkit[fastapi] @ git+https://github.com/SeaViewer91/xferkit.git@vX.Y.Z"
```

and load the browser client from the Python package (`mount_browser_client`)
or from `https://cdn.jsdelivr.net/gh/SeaViewer91/xferkit@vX.Y.Z/src/xferkit/static/xferkit.mjs`.
The bundled `src/xferkit/static/xferkit.mjs` is committed, so installing from
git needs no Node.js.

Pushing a tag `vX.Y.Z` runs [.github/workflows/release.yml](.github/workflows/release.yml):
it checks that the tag matches every version number, runs all tests (including
real Chromium), builds the wheel and sdist, and creates a GitHub Release with
them attached and the CHANGELOG section as release notes. Nothing is uploaded
to PyPI or npm.

## Every release

1. Set the version in `src/xferkit/_version.py`, `clients/js/package.json`
   and `VERSION` in `clients/js/src/index.ts`; add a `## X.Y.Z — date`
   section to `CHANGELOG.md`. `python scripts/check_release_version.py vX.Y.Z`
   checks all four.
2. Rebuild the browser client if it changed (`cd clients/js && npm run build`),
   so the copy in `src/xferkit/static/` is current.
3. Update the version in the install lines of `README.md`, `README.ko.md` and
   `clients/js/README.md`.
4. Commit, push, wait for CI to pass, then tag:

   ```bash
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```

5. Check *Actions → Release* and the new entry under *Releases*.

A tag that was used should not be moved: installs pinned to it would silently
change. Fix mistakes with a new patch version.

## Later: PyPI and npm

The package metadata is ready for both registries (`python -m build` and
`npm pack` produce clean packages). When publishing starts, add publish jobs
using trusted publishing (no stored tokens): `pypa/gh-action-pypi-publish`
with a PyPI "pending publisher" for `release.yml`, and `npm publish` with
`id-token: write` after the first version was published by hand (npm can only
trust a workflow for a package that already exists).
