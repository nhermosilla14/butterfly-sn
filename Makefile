# Build, test and release helpers. GNU make; recipes run with /bin/sh and need no network.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
DIST ?= dist
BIN ?= bin
HOSTARCH := $(shell uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/')
TESTIMAGE := butterfly-sn-test:$(VERSION)-$(shell echo $$$$)

PKG := github.com/nhermosilla14/butterfly-sn
LDFLAGS := -s -w -X $(PKG)/internal/app.Version=$(VERSION)
GOBUILD := CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags "$(LDFLAGS)"

.PHONY: help version build dist image image-test test check-docs check clean

help:
	@echo "build       host binary in $(BIN)/"
	@echo "dist        cross-built binaries, archives and SHA256SUMS in $(DIST)/"
	@echo "image       multi-arch (amd64, arm64) scratch image manifest butterfly-sn:$(VERSION), built with podman"
	@echo "image-test  build the host-arch image and smoke-test it with podman (no Docker)"
	@echo "test        formatting, vet, build and tests (code checks only)"
	@echo "check-docs  merge gate: docs/branch must be empty (fails while branch records exist)"
	@echo "check       test plus check-docs"
	@echo "clean       remove $(DIST)/ and $(BIN)/"
	@echo "version     print the resolved VERSION"

version:
	@echo "$(VERSION)"

build:
	$(GOBUILD) -o "$(BIN)/butterfly-sn" ./cmd/butterfly-sn

dist:
	rm -rf "$(DIST)"
	mkdir -p "$(DIST)"
	for t in linux/amd64 linux/arm64 windows/amd64; do \
		os=$${t%/*}; arch=$${t#*/}; ext=""; [ "$$os" = windows ] && ext=.exe; \
		name="butterfly-sn_$(VERSION)_$${os}_$${arch}"; \
		GOOS=$$os GOARCH=$$arch $(GOBUILD) -o "$(DIST)/$$os-$$arch/butterfly-sn$$ext" ./cmd/butterfly-sn || exit 1; \
		stage="$(DIST)/.stage/$$name"; \
		mkdir -p "$$stage"; \
		cp "$(DIST)/$$os-$$arch/butterfly-sn$$ext" LICENSE config.example.yaml "$$stage/"; \
		chmod 755 "$$stage" "$$stage/butterfly-sn$$ext"; chmod 644 "$$stage/LICENSE" "$$stage/config.example.yaml"; \
		if [ "$$os" = windows ]; then \
			find "$$stage" -exec touch -d '2020-01-01 00:00:00 UTC' {} +; \
			(cd "$(DIST)/.stage" && find "$$name" | LC_ALL=C sort | TZ=UTC zip -X -q "../$$name.zip" -@) || exit 1; \
		else \
			(cd "$(DIST)/.stage" && tar --sort=name --mtime='UTC 2020-01-01' --owner=0 --group=0 --numeric-owner \
				-cf - "$$name" | gzip -n > "../$$name.tar.gz") || exit 1; \
		fi; \
	done
	rm -rf "$(DIST)/.stage"
	cd "$(DIST)" && sha256sum butterfly-sn_*.tar.gz butterfly-sn_*.zip | LC_ALL=C sort -k2 > SHA256SUMS

# The Containerfile copies from dist/, so the image targets only work with the default DIST.
image: dist
	@[ "$(DIST)" = dist ] || { echo "image: DIST must be 'dist' (the Containerfile copies from dist/), got '$(DIST)'"; exit 1; }
	podman manifest rm "butterfly-sn:$(VERSION)" >/dev/null 2>&1 || podman rmi "butterfly-sn:$(VERSION)" >/dev/null 2>&1 || true
	podman build --platform linux/amd64,linux/arm64 --manifest "butterfly-sn:$(VERSION)" --build-arg VERSION=$(VERSION) .

# image-test always removes the test image, even when the tests fail.
image-test: dist
	@[ "$(DIST)" = dist ] || { echo "image-test: DIST must be 'dist' (the Containerfile copies from dist/), got '$(DIST)'"; exit 1; }
	@img="$(TESTIMAGE)"; \
	podman build --platform linux/$(HOSTARCH) -t "$$img" --build-arg VERSION=$(VERSION) . || { podman rmi -f "$$img" >/dev/null 2>&1; exit 1; }; \
	BUTTERFLY_SN_IMAGE="$$img" BUTTERFLY_SN_IMAGE_VERSION="$(VERSION)" go test -tags image -count=1 -v ./test/image/...; rc=$$?; \
	podman rmi -f "$$img" >/dev/null 2>&1; exit $$rc

# test runs the code checks only. check-docs is separate because it is the merge gate and is expected to fail
# while branch records exist under docs/branch.
test:
	@out=$$(gofmt -l . butterfly-mqtt-client); [ -z "$$out" ] || { echo "gofmt needed:"; echo "$$out"; exit 1; }
	go vet ./...
	go build ./...
	go test -race -count=1 ./...
	cd butterfly-mqtt-client && go vet ./... && go build ./... && go test -race -count=1 ./...

check-docs:
	sh scripts/check-branch-docs.sh

check: test check-docs

clean:
	rm -rf "$(DIST)" "$(BIN)"
