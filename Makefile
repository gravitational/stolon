BINS := bin/stolon-keeper bin/stolon-proxy bin/stolon-sentinel bin/stolonctl

.PHONY: all
all:
	for cmd in stolonctl sentinel proxy keeper; do go install github.com/gravitational/stolon/cmd/"$$cmd"; done

.PHONY: run
run: $(BINS)
	goreman start

$(BINS):
	./build

.PHONY: clean
clean:
	rm -rf $(BINS)

.PHONY: test
test:
	go test -v -timeout 3m -cover -race ./cmd/... ./pkg/...

