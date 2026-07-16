CC = gcc
CFLAGS = -Wall -Wextra -O2 -fPIC -D_GNU_SOURCE
INCLUDES = -Istorage/c/include
LIBDIR = storage/lib

STORAGE_SRCS = storage/c/commitlog/commit_log.c storage/c/index/sparse_index.c
STORAGE_OBJS = $(STORAGE_SRCS:.c=.o)
STORAGE_TARGET = $(LIBDIR)/libstorage.a

GO_DIR = cluster/go
GO_BIN = $(GO_DIR)/bin

.PHONY: all build clean storage broker nameserver

all: build

build: storage broker nameserver

storage: $(STORAGE_TARGET)

$(STORAGE_TARGET): $(STORAGE_OBJS)
	@mkdir -p $(LIBDIR)
	ar rcs $@ $^

%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

broker: storage
	@mkdir -p $(GO_BIN)
	cd $(GO_DIR)/broker && CGO_ENABLED=1 go build -tags=linux -o $(GO_BIN)/broker .

nameserver:
	@mkdir -p $(GO_BIN)
	cd $(GO_DIR)/nameserver && go build -o $(GO_BIN)/nameserver .

clean:
	rm -f $(STORAGE_OBJS)
	rm -rf $(LIBDIR)
	rm -rf $(GO_BIN)