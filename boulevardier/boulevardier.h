#ifndef BOULEVARDIER_H
#define BOULEVARDIER_H

typedef struct {
    size_t ksize;
    size_t vsize;
} item_header;

class Boulevardier {
public:
    Boulevardier(const char* logname);

    int BlvdWrite(item_header* header, const char* kdata,
                  const char* vdata, size_t* offset);

    int BlvdGet(size_t offset, char** data, size_t* len);

private:
    std::string _logname;
    int _log; // fd 
};

#endif
