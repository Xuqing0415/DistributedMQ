#ifndef MQ_STORAGE_H
#define MQ_STORAGE_H

#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MQ_SUCCESS 0
#define MQ_ERROR -1
#define MQ_NOT_FOUND -2
#define MQ_IO_ERROR -3
#define MQ_MEM_ERROR -4

typedef struct {
    uint64_t offset;
    uint32_t size;
    uint64_t timestamp;
} IndexEntry;

typedef struct CommitLog CommitLog;
typedef struct SparseIndex SparseIndex;

CommitLog* commit_log_open(const char* path, uint64_t file_size);
void commit_log_close(CommitLog* cl);
int commit_log_append(CommitLog* cl, const char* data, uint32_t size, uint64_t* offset);
int commit_log_read(CommitLog* cl, uint64_t offset, char* buf, uint32_t size);
uint64_t commit_log_get_offset(CommitLog* cl);
int commit_log_sync(CommitLog* cl);

SparseIndex* sparse_index_open(const char* path);
void sparse_index_close(SparseIndex* idx);
int sparse_index_put(SparseIndex* idx, uint64_t offset, uint32_t size, uint64_t timestamp);
int sparse_index_get(SparseIndex* idx, uint64_t offset, IndexEntry* entry);
int sparse_index_lookup(SparseIndex* idx, uint64_t timestamp, IndexEntry* entry);
uint64_t sparse_index_get_last_offset(SparseIndex* idx);
uint64_t sparse_index_get_entry_count(SparseIndex* idx);
void sparse_index_get_entry_at(SparseIndex* idx, int index, IndexEntry* entry);

int commit_log_cleanup(const char* segment_dir, uint64_t retention_ms, uint64_t retention_bytes);

#ifdef __cplusplus
}
#endif

#endif
