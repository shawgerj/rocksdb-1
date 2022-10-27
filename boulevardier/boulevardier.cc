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
    ssize_t haveread, total;
    haveread = total = 0;

    for (total = 0; total < (ssize_t)size; total += haveread) {
        haveread = read(fd, buf + haveread, size - haveread);
        if (haveread < 0)
            return -1;
    }

    return 0;
}

// append to log
int Boulevardier::BlvdWrite(item_header* header, const char* kdata,
                            const char* vdata, size_t* offset) {
    off_t off;
    if ((off = lseek(_log, 0, SEEK_END)) < 0) {
        std::cout << "Error seeking log" << std::endl;
        return -1;
    }
    *offset = (size_t)off;

    if (safe_write(_log, (const char*)header, sizeof(item_header)) < 0)
        std::cout << strerror(errno) << std::endl;
    if (safe_write(_log, kdata, header->ksize) < 0)
        std::cout << strerror(errno) << std::endl;
    if (safe_write(_log, vdata, header->vsize) < 0)
        std::cout << strerror(errno) << std::endl;

    return 0;
}

int Boulevardier::BlvdGet(size_t offset, char** data, size_t* len) {
    item_header *header = (item_header*)malloc(sizeof(item_header));
    
    lseek(_log, offset, SEEK_SET);

    safe_read(_log, (char*)header, sizeof(item_header));

    char *kbuf = (char*)malloc(header->ksize * sizeof(char));
    char *vbuf = (char*)malloc(header->vsize * sizeof(char));

    if (safe_read(_log, kbuf, header->ksize) < 0)
        std::cout << strerror(errno) << std::endl;
    if (safe_read(_log, vbuf, header->vsize) < 0)
        std::cout << strerror(errno) << std::endl;

    *data = vbuf;
    *len = header->vsize;
    free(kbuf);

    return 0;
}
    
