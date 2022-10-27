#ifndef BOULEVARDIER_H
#define BOULEVARDIER_H

class Boulevardier {
public:
    Boulevardier(const char* logname);

    int LogAppend(const char* kdata, size_t ksize, const char* vdata,
                  size_t vsize, size_t* offset);
    int Get(size_t offset, char** data, size_t* len);

private:
    std::string _logname;
    int _log; // fd 
};

#endif
