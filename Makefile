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

# Java/Maelstrom (which is Clojure) tmp dir
TMP_DIR      := .state

.PHONY: all build run bench

all: build

build:
	cargo build --release --bin $(BINARY)

# Run the Maelstrom test
# Use like: make run NODES=5 TIME_LIMIT=60
run: build
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

bench:
	cargo +nightly bench
