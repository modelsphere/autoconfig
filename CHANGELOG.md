# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). One version tag
releases the controller image and both sidecar images under the same number.

## [Unreleased]

### Added
- CI on every pull request: `gofmt`, `go vet`, `make test` with a check that
  generated files are committed, `helm lint` of the chart, a docker build, and
  a gate for the license text and committed credentials.
- Dependabot for Go modules and the GitHub Actions, which are pinned to commit
  SHAs.
- `NOTICE`.
- English `README.md` and `DEPLOY.md`; the Chinese originals are kept as
  `README.zh-CN.md` and `DEPLOY.zh-CN.md`.

### Changed
- The Makefile, the kustomize manager config and the Helm chart default to
  the public images (`4pdosc/autoconfig*`); `make docker-build` passes no
  build-arg overrides by default.
- `DEPLOY.md` installs the charts from the public Helm repository
  (`https://modelsphere.github.io/helm-charts`).
- The e2e scripts and samples use public images. `MON_IMG` has no public
  default and must be set.

### Removed
- The internal GitLab pipeline (`.gitlab-ci.yml`).

### Fixed
- The arm64 images contained an amd64 binary: the Dockerfiles fixed
  `GOARCH=amd64`. They now build for the target platform.

## [0.4.0] - 2026-09-25

### Changed
- **Breaking:** `ModelRoute` moves from `routing.gpucluster.io` to
  `routing.modelsphere.dev`, and the controller reads `LLMSLORequirement` from
  `inference.modelsphere.dev` instead of `inference.x-k8s.io`. The finalizer
  and the leader-election ID follow the routing group. Objects created under
  the old groups are not picked up.

## [0.3.47] - 2026-09-22

### Added
- Release workflow: a version tag builds `autoconfig`, `autoconfig-reload`
  and `autoconfig-hagate` for linux/amd64 and linux/arm64 and pushes them to
  Docker Hub (`4pdosc/`).

## [0.3.46] - 2026-09-18

### Added
- The reload sidecar accepts `--watch` more than once, so changes in several
  mounted volumes (for example a route ConfigMap and a key Secret) reload the
  same process. One path behaves as before.
- Apache-2.0 `LICENSE`.

### Changed
- Base images and the Go module proxy are Dockerfile build args that default
  to public sources.
- README rewritten for readers outside the original team.
- The e2e scripts take the openresty auth key from `AUTH_KEY`, which must be
  set.

### Fixed
- The chart pinned `image.tag` to 0.3.45, so upgrading the chart kept the old
  controller image. It is empty again and falls back to `appVersion`.

## [0.3.45] - 2026-09-16

First tagged release. Earlier versions were built from this history without
tags.

### Added
- `spec.cart.outputKey`: the ConfigMap key autoconfig writes CART workers to
  (default `config.yaml`). Pointing it at a workers-only key leaves the base
  config key to the chart.
- Already in place at this release: the `ModelRoute` CRD with CEL validation;
  backend discovery from EndpointSlices or pod labels; rendering of openresty
  routes (`llm` and `video` model types), CART workers and monitor targets into
  existing ConfigMaps; the `autoconfig-reload` and `autoconfig-hagate`
  sidecars; a Helm chart and kustomize manifests.

[Unreleased]: https://github.com/modelsphere/autoconfig/compare/0.4.0...HEAD
[0.4.0]: https://github.com/modelsphere/autoconfig/compare/0.3.47...0.4.0
[0.3.47]: https://github.com/modelsphere/autoconfig/compare/0.3.46...0.3.47
[0.3.46]: https://github.com/modelsphere/autoconfig/compare/0.3.45...0.3.46
[0.3.45]: https://github.com/modelsphere/autoconfig/releases/tag/0.3.45
