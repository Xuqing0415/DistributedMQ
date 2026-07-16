CC = gcc
CFLAGS = -Wall -Wextra -O2 -fPIC
INCLUDES = -Iinclude
LIBDIR = ../lib

SRCS_COMMITLOG = commitlog/commit_log.c
SRCS_INDEX = index/sparse_index.c

OBJS_COMMITLOG = $(SRCS_COMMITLOG:.c=.o)
OBJS_INDEX = $(SRCS_INDEX:.c=.o)

TARGET = $(LIBDIR)/libstorage.a

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(OBJS_COMMITLOG) $(OBJS_INDEX)
	@mkdir -p $(LIBDIR)
	ar rcs $@ $^

%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

clean:
	rm -f $(OBJS_COMMITLOG) $(OBJS_INDEX)
	rm -rf $(LIBDIR)
