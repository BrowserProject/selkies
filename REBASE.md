# BrowserProject/selkies

The Selkies fork of BrowserProject: upstream
[selkies-project/selkies](https://github.com/selkies-project/selkies) at one
base commit plus BrowserProject's changes on top, one commit each, every
subject starting with `bp`. Default branch `bp/main`; `main` mirrors upstream.

## What the commits on top hold

| Where | What |
|---|---|
| `bp: normalize the punctuation ...`, `bp 0001` to `bp 0035` | The patches the BrowserProject images applied to the vendored tarball, in their order and with their documentation as commit messages (no `0025`; none was made unnecessary by upstream's `SELKIES_UNIX_SOCKET`, no patch touched listening) |
| `src/selkies/connection_scopes.py` | Per-connection scopes from the relay (`SELKIES_CONNECTION_SCOPES`, header `X-BP-Scopes`), held on every channel |
| `tests/bp/` | The patch tests (`tests.list`), run by `bp/run-tests.sh` in `bp/test.Dockerfile` |
| `addons/selkies-web-core/` | BrowserProject's viewer cores, in upstream's place so a rebase merges upstream's client changes into them |
| `bp/client/` | The `@browserproject/selkies-client` package of those cores, with `.d.ts` types |
| `.github/workflows/bp-*.yml` | This fork's CI and releases; upstream's automatic workflows (`ci.yaml`, `docs.yaml`, `devcontainer-feature.yaml`) are removed, its manual ones are unused |

The product modules (`stream_profiles.py`, `stream_control.py` and the others)
are not here: they are the package `containers/browser-base/selkies-ext` of
BrowserProject/browser, installed into this fork's `selkies` package beside
the wheel. The fork imports them as `selkies.<module>`, so most tests here run
only with them installed. CI runs the suites when the secret
`BP_SELKIES_EXT_TOKEN` (read-only contents of BrowserProject/browser) is set
and otherwise says so in a warning; BrowserProject/browser runs them against
every pinned release.

## Rebase onto a new upstream commit

1. `git fetch upstream && git checkout -b bp/rebase-<new short sha> bp/main`
2. `git rebase --onto <new sha> <old base sha>`: resolve each conflict inside its own commit. A conflict on a removed upstream workflow is resolved with `git rm`.
3. A patch that upstream made unnecessary is dropped from the rebase (an interactive rebase, or `git rebase --onto` around it), and the release notes name it with the reason.
4. Review upstream's changes to the client (`git diff <old base> <new sha> -- addons/selkies-web-core`) against the hook markers of the cores, and its changes to `pyproject.toml` dependencies.
5. Push the branch: `bp CI` runs on `bp/rebase-*`. When it is green, `git push --force-with-lease origin bp/rebase-<new short sha>:bp/main` and release. Older releases stay reachable through their tags.

Changes between rebases are pull requests to `bp/main`, each a `bp` commit.

## Release

Tag `bp-<upstream short sha>-<n>` on `bp/main` (`n` counts from 1 per base)
and push the tag. `bp release` checks that the base is the tag's ancestor and
that every commit on top is a `bp` commit, runs the CI, attests the wheel and
the client package, publishes `@browserproject/selkies-client` to GitHub
Packages and creates the GitHub release with the changelog. Versions
(`bp/version.sh`): `bp-7a1219d-1` is the wheel `0.0.0+bp.g7a1219d.1` and the
npm package `0.0.0-bp.g7a1219d.1` (dist-tag `bp`).

BrowserProject/browser pins the release in `containers/browser-base/selkies.version`.

## License

MPL-2.0, like upstream: every file keeps its license and new files carry the
MPL header. The viewer cores reach every browser that opens a session, so the
source of each release must stay available: this repository is public and
every release is a tag in it.
