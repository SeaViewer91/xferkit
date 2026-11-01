# Releasing

Pushing a tag `vX.Y.Z` runs [.github/workflows/release.yml](.github/workflows/release.yml):
it checks that the tag matches every version number, runs all tests (including
real Chromium), then publishes the Python package to PyPI and the browser
client to npm. No tokens are stored anywhere: both registries trust that
workflow through OpenID Connect ("trusted publishing").

## One-time setup

1. **GitHub environments.** In the repository: *Settings → Environments → New
   environment*, create `pypi` and `npm`. Optionally add yourself as a
   *required reviewer* to each, so every publish waits for your click.
2. **PyPI.** Log in at https://pypi.org (enable 2FA), open
   *Your account → Publishing → Add a new pending publisher* and enter:
   PyPI project name `xferkit`, owner `SeaViewer91`, repository `xferkit`,
   workflow `release.yml`, environment `pypi`. The project is created by the
   first release.
3. **npm.** npm only lets you configure trusted publishing for a package that
   already exists, so the very first version is published by hand once:

   ```bash
   cd clients/js
   npm login                      # account with 2FA
   npm ci && npm run build
   npm publish --access public    # asks for the 2FA code
   ```

   Then on https://www.npmjs.com/package/xferkit → *Settings → Trusted
   publishing*, choose GitHub Actions: user `SeaViewer91`, repository
   `xferkit`, workflow `release.yml`, environment `npm`. Afterwards set
   *Publishing access* to "Require two-factor authentication and disallow
   tokens". The release workflow skips npm versions that already exist, so the
   hand-published first version does not break the first tagged release.

## Every release

1. Set the version in `src/xferkit/_version.py`, `clients/js/package.json`
   and `VERSION` in `clients/js/src/index.ts`; add a `## X.Y.Z — date`
   section to `CHANGELOG.md`. `python scripts/check_release_version.py vX.Y.Z`
   checks all four.
2. Rebuild the browser client if it changed (`cd clients/js && npm run build`),
   so the copy in `src/xferkit/static/` is current.
3. Commit, push, wait for CI to pass, then tag:

   ```bash
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```

4. Watch *Actions → Release*. If a publish job fails after the other one
   succeeded, fix the cause and *Re-run failed jobs*: versions already
   published are skipped.

A version number can be published only once on PyPI (even after deleting it)
and on npm, so fix mistakes with a new patch version.
