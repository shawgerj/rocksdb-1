#ifndef BOULEVARDIER_H
#define BOULEVARDIER_H

#include <vector>

typedef struct {
    size_t ksize;
    size_t vsize;
} item_header;

class Boulevardier {
public:
  Boulevardier(const char* logname);

  int BlvdWrite(std::string& logdata, std::vector<size_t>* offsets);
  int BlvdGet(size_t offset, char** data, size_t* len);

private:
    std::string _logname;
    int _log; // fd 
};

#endif
