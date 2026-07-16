#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <errno.h>

#include "mq_storage.h"

#define INDEX_ENTRY_SIZE sizeof(IndexEntry)
#define MAX_ENTRIES (1024 * 1024 * 10)

struct SparseIndex {
    int fd;
    char* path;
    char* mmap_addr;
    size_t mmap_len;
    uint64_t entry_count;
    uint64_t last_offset;
};

SparseIndex* sparse_index_open(const char* path) {
    SparseIndex* idx = (SparseIndex*)malloc(sizeof(SparseIndex));
    if (!idx) return NULL;

    idx->path = strdup(path);
    if (!idx->path) {
        free(idx);
        return NULL;
    }

    idx->fd = open(path, O_RDWR | O_CREAT, 0644);
    if (idx->fd < 0) {
        free(idx->path);
        free(idx);
        return NULL;
    }

    struct stat st;
    if (fstat(idx->fd, &st) < 0) {
        close(idx->fd);
        free(idx->path);
        free(idx);
        return NULL;
    }

    idx->mmap_len = MAX_ENTRIES * INDEX_ENTRY_SIZE;
    if ((size_t)st.st_size < idx->mmap_len) {
        if (ftruncate(idx->fd, idx->mmap_len) < 0) {
            close(idx->fd);
            free(idx->path);
            free(idx);
            return NULL;
        }
    }

    idx->mmap_addr = mmap(NULL, idx->mmap_len, PROT_READ | PROT_WRITE, MAP_SHARED, idx->fd, 0);
    if (idx->mmap_addr == MAP_FAILED) {
        close(idx->fd);
        free(idx->path);
        free(idx);
        return NULL;
    }

    idx->entry_count = st.st_size / INDEX_ENTRY_SIZE;
    
    if (idx->entry_count > 0) {
        IndexEntry* last = (IndexEntry*)(idx->mmap_addr + (idx->entry_count - 1) * INDEX_ENTRY_SIZE);
        idx->last_offset = last->offset;
    } else {
        idx->last_offset = 0;
    }

    return idx;
}

void sparse_index_close(SparseIndex* idx) {
    if (!idx) return;

    if (idx->mmap_addr != MAP_FAILED) {
        munmap(idx->mmap_addr, idx->mmap_len);
    }
    if (idx->fd >= 0) {
        fsync(idx->fd);
        close(idx->fd);
    }
    if (idx->path) {
        free(idx->path);
    }
    free(idx);
}

int sparse_index_put(SparseIndex* idx, uint64_t offset, uint32_t size, uint64_t timestamp) {
    if (!idx) return MQ_ERROR;

    if (idx->entry_count >= MAX_ENTRIES) {
        return MQ_ERROR;
    }

    IndexEntry* entry = (IndexEntry*)(idx->mmap_addr + idx->entry_count * INDEX_ENTRY_SIZE);
    entry->offset = offset;
    entry->size = size;
    entry->timestamp = timestamp;

    idx->entry_count++;
    idx->last_offset = offset;

    return MQ_SUCCESS;
}

int sparse_index_get(SparseIndex* idx, uint64_t offset, IndexEntry* entry) {
    if (!idx || !entry) return MQ_ERROR;

    for (uint64_t i = 0; i < idx->entry_count; i++) {
        IndexEntry* e = (IndexEntry*)(idx->mmap_addr + i * INDEX_ENTRY_SIZE);
        if (e->offset == offset) {
            *entry = *e;
            return MQ_SUCCESS;
        }
    }

    return MQ_NOT_FOUND;
}

int sparse_index_lookup(SparseIndex* idx, uint64_t timestamp, IndexEntry* entry) {
    if (!idx || !entry) return MQ_ERROR;

    if (idx->entry_count == 0) {
        return MQ_NOT_FOUND;
    }

    uint64_t left = 0;
    uint64_t right = idx->entry_count - 1;
    uint64_t result = idx->entry_count;

    while (left <= right) {
        uint64_t mid = (left + right) / 2;
        IndexEntry* e = (IndexEntry*)(idx->mmap_addr + mid * INDEX_ENTRY_SIZE);
        
        if (e->timestamp >= timestamp) {
            result = mid;
            if (mid == 0) break;
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }

    if (result == idx->entry_count) {
        return MQ_NOT_FOUND;
    }

    *entry = *((IndexEntry*)(idx->mmap_addr + result * INDEX_ENTRY_SIZE));
    return MQ_SUCCESS;
}

uint64_t sparse_index_get_last_offset(SparseIndex* idx) {
	return idx ? idx->last_offset : 0;
}

uint64_t sparse_index_get_entry_count(SparseIndex* idx) {
	return idx ? idx->entry_count : 0;
}

void sparse_index_get_entry_at(SparseIndex* idx, int index, IndexEntry* entry) {
	if (!idx || !entry || index < 0 || (uint64_t)index >= idx->entry_count) {
		return;
	}
	IndexEntry* e = (IndexEntry*)(idx->mmap_addr + index * INDEX_ENTRY_SIZE);
	entry->offset = e->offset;
	entry->size = e->size;
	entry->timestamp = e->timestamp;
}
