BINARY       := raft
TARGET_DIR   := target/release
BIN_PATH     := $(TARGET_DIR)/$(BINARY)

# Maelstrom config
WORKLOAD     ?= lin-kv
NODES        ?= 3
TIME_LIMIT   ?= 120
CONCURRENCY  ?= 6n
RATE         ?= 10
NEMESIS      ?= partition
NEMESIS_INT  ?= 10
TEST_COUNT   ?= 1

# Java/Maelstrom (which is Clojure) tmp dir. Maelstrom runs our binary inside this tmp
# dir. Controlling its location is convenient.
TMP_DIR      := .state

.PHONY: all build run bench

all: build

build:
	cargo build --release --bin $(BINARY)

# Run the Maelstrom test
# Use like: make run NODES=5 TIME_LIMIT=60
run: build docker-compose
	rm -rf $(TMP_DIR)
	mkdir -p $(TMP_DIR)
	_JAVA_OPTIONS="-Djava.io.tmpdir=$(CURDIR)/$(TMP_DIR)" maelstrom test \
		--workload $(WORKLOAD) \
		--bin ./$(BIN_PATH) \
		--time-limit $(TIME_LIMIT) \
		--node-count $(NODES) \
		--concurrency $(CONCURRENCY) \
		--rate $(RATE) \
		--nemesis-interval $(NEMESIS_INT) \
		--nemesis $(NEMESIS) \
		--test-count $(TEST_COUNT)

diff:
	cargo xtask jdiff .state/node/n0 .state/node/n1 || true
	@echo ""
	cargo xtask jdiff .state/node/n0 .state/node/n2 || true
	@echo ""
	cargo xtask jdiff .state/node/n1 .state/node/n2 || true

# https://github.com/josephburnett/jd
jdiff:
	jd .state/node/n0 .state/node/n1 || true
	@echo ""
	jd .state/node/n0 .state/node/n2 || true
	@echo ""
	jd .state/node/n1 .state/node/n2 || true

bench:
	cargo +nightly bench

docker-compose:
	docker compose up --detach
