#include <iostream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <string.h>
#include "boulevardier.h"

Boulevardier::Boulevardier(const char* logname) {
    _logname = std::string(logname);
    if ((_log = open(logname, O_RDWR | O_CREAT | O_APPEND, S_IRWXU)) < 0) {
        std::cout << "Error opening logfile" << std::endl;
        exit(1);
    }
}

int safe_write(int fd, const char* data, size_t size) {
    ssize_t written = 0;

    for (ssize_t total = 0; total < (ssize_t)size; total += written) {
        written = write(fd, data + written, size - written);
        if (written < 0)
            return -1;
    }
    return 0;
}

int safe_read(int fd, char* buf, size_t size) {
    ssize_t haveread = 0;

    for (ssize_t total = 0; total < (ssize_t)size; total += haveread) {
        haveread = read(fd, buf + haveread, size - haveread);
        if (haveread < 0)
            return -1;
    }
    return 0;
}

int Boulevardier::LogAppend(const char* kdata, size_t ksize,
                            const char* vdata, size_t vsize, size_t* offset) {
    off_t off;
    if ((off = lseek(_log, 0, SEEK_CUR)) < 0) {
        std::cout << "Error seeking log" << std::endl;
        return -1;
    }
    *offset = (size_t)off;

    if (safe_write(_log, (const char*)&ksize, sizeof(ksize)) < 0)
        std::cout << strerror(errno) << std::endl;
    if (safe_write(_log, (const char*)&vsize, sizeof(vsize)) < 0)
        std::cout << strerror(errno) << std::endl;
    if (safe_write(_log, kdata, ksize) < 0)
        std::cout << strerror(errno) << std::endl;
    if (safe_write(_log, vdata, vsize) < 0)
        std::cout << strerror(errno) << std::endl;

    return 0;
}

int Boulevardier::Get(size_t offset, char** data, size_t* len) {
    size_t ksize;
    size_t vsize;
    
    lseek(_log, offset, SEEK_SET);

    assert(read(_log, &ksize, sizeof(size_t)) != -1);
    assert(read(_log, &vsize, sizeof(size_t)) != -1);

    char *kbuf = (char*)malloc(ksize * sizeof(char));
    char *vbuf = (char*)malloc(vsize * sizeof(char));

    if (safe_read(_log, kbuf, ksize) < 0)
        std::cout << strerror(errno) << std::endl;
    if (safe_read(_log, vbuf, vsize) < 0)
        std::cout << strerror(errno) << std::endl;

    lseek(_log, offset, SEEK_END);

    *data = vbuf;
    *len = vsize;
    free(kbuf);

    return 0;
}
    
