BINS := bin/stolon-keeper bin/stolon-proxy bin/stolon-sentinel bin/stolonctl
PG_BINS_PATH ?= /usr/lib/postgresql/9.5/bin

.PHONY: all
all: clean $(BINS)

$(BINS):
	./build

.PHONY: run
run: $(BINS)
	goreman start

.PHONY: clean
clean:
	rm -rf $(BINS)

.PHONY: test
test:
	./test -v

.PHONY: test-integration
test-integration:
	PATH=$(PG_BINS_PATH):$$PATH \
	INTEGRATION=1 \
	STOLON_TEST_STORE_BACKEND=etcd \
	ETCD_BIN=$$GOPATH/src/github.com/coreos/etcd/bin/etcd \
	./test -v

.PHONY: install-dev-tools
install-dev-tools:
	go get github.com/tools/godep
	go get github.com/mattn/goreman
