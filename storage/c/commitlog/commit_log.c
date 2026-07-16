#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>
#include <dirent.h>
#include <time.h>

#include "mq_storage.h"

#define DEFAULT_FILE_SIZE (1024 * 1024 * 1024ULL)

struct CommitLog {
    int fd;
    char* path;
    uint64_t file_size;
    uint64_t write_offset;
    char* mmap_addr;
    size_t mmap_len;
};

CommitLog* commit_log_open(const char* path, uint64_t file_size) {
    CommitLog* cl = (CommitLog*)malloc(sizeof(CommitLog));
    if (!cl) return NULL;

    cl->path = strdup(path);
    if (!cl->path) {
        free(cl);
        return NULL;
    }

    cl->file_size = file_size > 0 ? file_size : DEFAULT_FILE_SIZE;
    cl->fd = open(path, O_RDWR | O_CREAT | O_APPEND, 0644);
    if (cl->fd < 0) {
        free(cl->path);
        free(cl);
        return NULL;
    }

    struct stat st;
    if (fstat(cl->fd, &st) < 0) {
        close(cl->fd);
        free(cl->path);
        free(cl);
        return NULL;
    }

    cl->write_offset = st.st_size;

    if (st.st_size < (off_t)cl->file_size) {
        if (ftruncate(cl->fd, cl->file_size) < 0) {
            close(cl->fd);
            free(cl->path);
            free(cl);
            return NULL;
        }
    }

    cl->mmap_len = (size_t)cl->file_size;
    cl->mmap_addr = mmap(NULL, cl->mmap_len, PROT_READ | PROT_WRITE, MAP_SHARED, cl->fd, 0);
    if (cl->mmap_addr == MAP_FAILED) {
        close(cl->fd);
        free(cl->path);
        free(cl);
        return NULL;
    }

    return cl;
}

void commit_log_close(CommitLog* cl) {
    if (!cl) return;

    if (cl->mmap_addr != MAP_FAILED) {
        munmap(cl->mmap_addr, cl->mmap_len);
    }
    if (cl->fd >= 0) {
        fsync(cl->fd);
        close(cl->fd);
    }
    if (cl->path) {
        free(cl->path);
    }
    free(cl);
}

int commit_log_append(CommitLog* cl, const char* data, uint32_t size, uint64_t* offset) {
    if (!cl || !data || size == 0) return MQ_ERROR;

    if (cl->write_offset + size > cl->file_size) {
        return MQ_ERROR;
    }

    memcpy(cl->mmap_addr + cl->write_offset, data, size);
    *offset = cl->write_offset;
    cl->write_offset += size;

    return MQ_SUCCESS;
}

int commit_log_read(CommitLog* cl, uint64_t offset, char* buf, uint32_t size) {
    if (!cl || !buf) return MQ_ERROR;

    if (offset + size > cl->write_offset) {
        return MQ_NOT_FOUND;
    }

    memcpy(buf, cl->mmap_addr + offset, size);
    return MQ_SUCCESS;
}

uint64_t commit_log_get_offset(CommitLog* cl) {
    return cl ? cl->write_offset : 0;
}

int commit_log_sync(CommitLog* cl) {
    if (!cl) return MQ_ERROR;

    if (msync(cl->mmap_addr, cl->mmap_len, MS_SYNC) < 0) {
        return MQ_IO_ERROR;
    }
    return MQ_SUCCESS;
}

int commit_log_cleanup(const char* segment_dir, uint64_t retention_ms, uint64_t retention_bytes) {
    DIR* dir = opendir(segment_dir);
    if (!dir) {
        return MQ_ERROR;
    }

    struct dirent* entry;
    uint64_t total_cleaned_bytes = 0;
    time_t now = time(NULL);
    uint64_t retention_sec = retention_ms / 1000;

    while ((entry = readdir(dir)) != NULL) {
        if (strncmp(entry->d_name, "segment_", 8) != 0) {
            continue;
        }

        char filepath[512];
        snprintf(filepath, sizeof(filepath), "%s/%s", segment_dir, entry->d_name);

        struct stat st;
        if (stat(filepath, &st) < 0) {
            continue;
        }

        if (retention_ms > 0 && (now - st.st_mtime) > (time_t)retention_sec) {
            if (unlink(filepath) == 0) {
                total_cleaned_bytes += st.st_size;
            }
            continue;
        }

        if (retention_bytes > 0 && st.st_size > retention_bytes) {
            if (unlink(filepath) == 0) {
                total_cleaned_bytes += st.st_size;
            }
            continue;
        }
    }

    closedir(dir);
    return (int)(total_cleaned_bytes / (1024 * 1024));
}
