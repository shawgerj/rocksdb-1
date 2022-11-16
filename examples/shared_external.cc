// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <algorithm>
#include <unistd.h>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "../util/random.h"
#include "boulevardier.h"

using namespace rocksdb;

std::string rootDir = "/tmp";
std::string kDBPath = rootDir + "/rocksdb_tests";
std::string kDBPath2 = rootDir + "/rocksdb_tests2";
Options dbOptions;

Random64 myrand(0);
const int key_size_ = sizeof(uint64_t);

enum WriteMode {
    RANDOM, SEQUENTIAL, UNIQUE_RANDOM
};

class KVPair {
public:
    KVPair(Slice& key, Slice& value)
        : _key(key),
          _value(value) {}

    Slice GetKey() { return _key; }
    Slice GetValue() { return _value; }

private:
    Slice _key;
    Slice _value;
};

// from db_bench_tool.cc
class KeyGenerator {
public:
    KeyGenerator(Random64* rand, WriteMode mode, uint64_t num,
                 uint64_t /*num_per_set*/ = 64 * 1024)
        : rand_(rand), mode_(mode), num_(num), next_(0) {
        if (mode_ == UNIQUE_RANDOM) {
            // NOTE: if memory consumption of this approach becomes a concern,
            // we can either break it into pieces and only random shuffle a section
            // each time. Alternatively, use a bit map implementation
            // (https://reviews.facebook.net/differential/diff/54627/)
            values_.resize(num_);
            for (uint64_t i = 0; i < num_; ++i) {
                values_[i] = i;
            }
            std::shuffle(
                values_.begin(), values_.end(),
                std::default_random_engine(static_cast<unsigned int>(0)));
        }
    }

    uint64_t Next() {
        switch (mode_) {
        case SEQUENTIAL:
            return next_++;
        case RANDOM:
            return rand_->Next() % num_;
        case UNIQUE_RANDOM:
            assert(next_ < num_);
            return values_[next_++];
        }
        assert(false);
        return std::numeric_limits<uint64_t>::max();
    }

private:
    Random64* rand_;
    WriteMode mode_;
    const uint64_t num_;
    uint64_t next_;
    std::vector<uint64_t> values_;
};

Slice AllocateKey(std::unique_ptr<const char[]>* key_guard) {
    char* data = new char[key_size_];
    const char* const_data = data;
    key_guard->reset(const_data);
    return Slice(key_guard->get(), key_size_);
}
// from db_bench_tool.cc. Simplified for my purposes
// If keys_per_prefix_ is 0, the key is simply a binary representation of
// random number followed by trailing '0's
//   ----------------------------
//   |        key 00000         |
//   ----------------------------
void GenerateKeyFromInt(uint64_t v, Slice* key) {
    char* start = const_cast<char*>(key->data());
    char* pos = start;
    int bytes_to_fill = std::min(key_size_, 8);
    memcpy(pos, static_cast<void*>(&v), bytes_to_fill);
    pos += bytes_to_fill;
    if (key_size_ > pos - start) {
        memset(pos, '0', key_size_ - (pos - start));
    }
}

// Helper for quickly generating random data.
// Taken from tools/db_bench_tool.cc
class RandomGenerator {
private:
    std::string data_;
    unsigned int pos_;

public:
    RandomGenerator() {
        // We use a limited amount of data over and over again and ensure
        // that it is larger than the compression window (32KB), and also
        // large enough to serve all typical value sizes we want to write.
        Random rnd(301);
        std::string piece;
        while (data_.size() < (unsigned)std::max(1048576, 32*1024)) {
            // Add a short fragment that is as compressible as specified
            // by FLAGS_compression_ratio.
            CompressibleString(&rnd, 0.5, 100, &piece);
            data_.append(piece);
        }
        pos_ = 0;
    }

    Slice Generate(unsigned int len) {
        assert(len <= data_.size());
        if (pos_ + len > data_.size()) {
            pos_ = 0;
        }
        pos_ += len;
        return Slice(data_.data() + pos_ - len, len);
    }

    Slice GenerateWithTTL(unsigned int len) {
        assert(len <= data_.size());
        if (pos_ + len > data_.size()) {
            pos_ = 0;
        }
        pos_ += len;
        return Slice(data_.data() + pos_ - len, len);
    }

    // RandomString() and CompressibleString() shamelessly copied from
    // ../test_util/testutil.cc into this class. Just for generating
    // random strings of data
    Slice RandomString(Random* rnd, int len, std::string* dst) {
        dst->resize(len);
        for (int i = 0; i < len; i++) {
            (*dst)[i] = static_cast<char>(' ' + rnd->Uniform(95));  // ' ' .. '~'
        }
        return Slice(*dst);
    }

    Slice CompressibleString(Random* rnd, double compressed_fraction,
                             int len, std::string* dst) {
        int raw = static_cast<int>(len * compressed_fraction);
        if (raw < 1) raw = 1;
        std::string raw_data;
        RandomString(rnd, raw, &raw_data);

        // Duplicate the random data until we have filled "len" bytes
        dst->clear();
        while (dst->size() < (unsigned int)len) {
            dst->append(raw_data);
        }
        dst->resize(len);
        return Slice(*dst);
    }
};

int testOneDBOneValue() {
    DB* db;
    Status s = DB::Open(dbOptions, kDBPath, &db);
    
    std::string logfile = rootDir + "/vlog1.txt";
    auto blvd = std::make_shared<Boulevardier>(logfile.c_str());
    db->SetBoulevardier(blvd.get());

    // Put key-value
    std::vector<size_t> offsets;
    WriteBatch batch;
    batch.Put("key1", "value1");
    s = db->Write(WriteOptions(), &batch, &offsets);
    assert(s.ok());

    std::string value;
    // get value
    s = db->GetExternal(ReadOptions(), "key1", &value);
    assert(s.ok());
    assert(value == "value1");

    delete db;
    DestroyDB(kDBPath, dbOptions);
    unlink(logfile.c_str());

    return 1;
}

int testOneDBMultiValue() {
    DB* db;
    Status s = DB::Open(dbOptions, kDBPath, &db);
    
    std::string logfile = rootDir + "/vlog1.txt";
    auto blvd = std::make_shared<Boulevardier>(logfile.c_str());
    db->SetBoulevardier(blvd.get());
    
    RandomGenerator gen;
    int value_size = 32;

    std::vector<KVPair> kvvec;
    for (int i = 0; i < 10; i++) {
        Slice key = gen.Generate(sizeof(int));
        Slice val = gen.Generate(value_size);
        kvvec.push_back(KVPair(key, val));
    }

    // Put key-values
    std::vector<size_t> offsets;
    WriteBatch batch;
    for (KVPair p : kvvec) {
        batch.Put(p.GetKey(), p.GetValue());
    }
    s = db->Write(WriteOptions(), &batch, &offsets);
    assert(s.ok());

    std::string value;
    // get values. Read backwards just in case there's a bug with only reading
    // sequentially
    for (int i = kvvec.size() - 1; i >= 0; i--) {
        s = db->GetExternal(ReadOptions(), kvvec[i].GetKey(), &value);
        assert(s.ok());
        assert(value == kvvec[i].GetValue());
    }

    delete db;
    DestroyDB(kDBPath, dbOptions);
//    unlink(logfile.c_str());

    return 1;
}

int testOneDBMultiValue2() {
    DB* db;
    Status s = DB::Open(dbOptions, kDBPath, &db);
    
    std::string logfile = rootDir + "/vlog1.txt";
    auto blvd = std::make_shared<Boulevardier>(logfile.c_str());
    db->SetBoulevardier(blvd.get());
    
    RandomGenerator gen;
    int value_size = 32;

    std::vector<KVPair> kvvec;
    for (int i = 0; i < 10; i++) {
        Slice key = gen.Generate(sizeof(int));
        Slice val = gen.Generate(value_size);
        kvvec.push_back(KVPair(key, val));
    }

    // Put key-values
    // this time, do it as separate batches
    std::vector<size_t> all_offsets;

    for (KVPair p : kvvec) {
        std::vector<size_t> offsets;
        WriteBatch batch;
        batch.Put(p.GetKey(), p.GetValue());
        s = db->Write(WriteOptions(), &batch, &offsets);
        assert(s.ok());
        for (size_t o : offsets) {
            all_offsets.push_back(o);
        }
    }
    assert(all_offsets.size() == kvvec.size());

    std::string value;
    // get values. Read backwards just in case there's a bug with only reading
    // sequentially
    for (int i = kvvec.size() - 1; i >= 0; i--) {
        s = db->GetExternal(ReadOptions(), kvvec[i].GetKey(), &value);
        assert(s.ok());
        assert(value == kvvec[i].GetValue());
    }

    delete db;
    DestroyDB(kDBPath, dbOptions);
    unlink(logfile.c_str());

    return 1;
}

int testOneDBMissingKey() {
    DB* db;
    Status s = DB::Open(dbOptions, kDBPath, &db);
    
    std::string logfile = rootDir + "/vlog1.txt";
    auto blvd = std::make_shared<Boulevardier>(logfile.c_str());
    db->SetBoulevardier(blvd.get());

    std::vector<size_t> offsets;
    assert(offsets.empty());
    WriteBatch batch;
    batch.Put("key1", "value1");
    batch.Put("key2long", "value2long");
    s = db->Write(WriteOptions(), &batch, &offsets);
    assert(s.ok());
    assert(offsets.size() == 2);

    std::string value;
    // get value
    s = db->GetExternal(ReadOptions(), "key2", &value);
    assert(!s.ok());

    delete db;
    DestroyDB(kDBPath, dbOptions);
    unlink(logfile.c_str());

    return 1;
}

int testTwoDBSharedOneValue() {
    DB* db1;
    DB* db2;
    Status s;
    s = DB::Open(dbOptions, kDBPath, &db1);
    s = DB::Open(dbOptions, kDBPath2, &db2);
    
    std::string logfile = rootDir + "/vlog1.txt";

    auto blvd = std::make_shared<Boulevardier>(logfile.c_str());
    db1->SetBoulevardier(blvd.get());
    db2->SetBoulevardier(blvd.get());

    // Put key-value
    std::vector<size_t> offset;
    WriteBatch batch;
    batch.Put("key1", "value1");
    s = db1->Write(WriteOptions(), &batch, &offset);
    assert(s.ok());
    WriteBatch batch2;
    batch2.Put("key1", std::to_string(offset[0]));
    s = db2->Write(WriteOptions(), &batch2);
    assert(s.ok());

    std::string value;
    // get value
    s = db1->GetExternal(ReadOptions(), "key1", &value);
    assert(s.ok());
    assert(value == "value1");
    s = db2->GetExternal(ReadOptions(), "key1", &value);
    assert(s.ok());
    assert(value == "value1");

    delete db1;
    delete db2;
    DestroyDB(kDBPath, dbOptions);
    DestroyDB(kDBPath2, dbOptions);
    unlink(logfile.c_str());
    return 1;
}

int testTwoDBSharedMultiValue() {
    DB* db1;
    DB* db2;
    Status s;
    s = DB::Open(dbOptions, kDBPath, &db1);
    s = DB::Open(dbOptions, kDBPath2, &db2);
    
    std::string logfile = rootDir + "/vlog1.txt";

    auto blvd = std::make_shared<Boulevardier>(logfile.c_str());
    db1->SetBoulevardier(blvd.get());
    db2->SetBoulevardier(blvd.get());

    RandomGenerator gen;
    int value_size = 32;

    std::vector<KVPair> kvvec;
    for (int i = 0; i < 10; i++) {
        Slice key = gen.Generate(sizeof(int));
        Slice val = gen.Generate(value_size);
        kvvec.push_back(KVPair(key, val));
    }

    // Put key-value
    WriteBatch batch;
    std::vector<size_t> offsets;
    for (KVPair p : kvvec) {
        batch.Put(p.GetKey(), p.GetValue());
    }
    s = db1->Write(WriteOptions(), &batch, &offsets);
    assert(s.ok());

    // Put in db2, but don't write values to log
    WriteBatch batch2;
    for (int i = 0; i < offsets.size(); i++) {
        batch2.Put(kvvec[i].GetKey(), std::to_string(offsets[i]));
    }
    s = db2->Write(WriteOptions(), &batch2);
    assert(s.ok());

    std::string value;
    // get values. Read backwards just in case there's a bug with only reading
    // sequentially
    for (int i = kvvec.size() - 1; i >= 0; i--) {
        s = db1->GetExternal(ReadOptions(), kvvec[i].GetKey(), &value);
        assert(s.ok());
        assert(value == kvvec[i].GetValue());
    }

    // and get values from db2, which never inserted values into the log
    for (int i = 0; i < kvvec.size(); i++) {
        s = db2->GetExternal(ReadOptions(), kvvec[i].GetKey(), &value);
        assert(s.ok());
        assert(value == kvvec[i].GetValue());
    }

    DestroyDB(kDBPath, dbOptions);
    DestroyDB(kDBPath2, dbOptions);
    return 1;
}


struct tArgs {
  size_t start;
  size_t num;
  DB* db;
};

void writeBody(struct tArgs* args) {
  for (size_t i = args->start; i < args->start + args->num; i++) {
    Status s;
    WriteBatch batch;
    std::vector<size_t> offsets;
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);

    batch.Put(key, value);
    s = args->db->Write(WriteOptions(), &batch, &offsets);
    assert(s.ok());
  }
}

int concurrentWriteTest(int n, size_t work) {
  std::vector<std::thread> threads;

  DB* db1;
  Status s;
  s = DB::Open(dbOptions, kDBPath, &db1);
    
  std::string logfile = rootDir + "/vlog1.txt";
  auto blvd = std::make_shared<Boulevardier>(logfile.c_str());
  db1->SetBoulevardier(blvd.get());
  

  struct tArgs* args = new tArgs[n];
  for (int i = 0; i < n; i++) {
    args[i].start = i * work;
    args[i].num = work;
    args[i].db = db1;
    threads.push_back(std::thread(writeBody, &args[i]));
 } 

  for (auto& t : threads) {
    t.join();
  }

  std::string value;
  for (size_t i = 0; i < n * work; i++) {
    std::string k = "key" + std::to_string(i);
    std::string expected_value = "value" + std::to_string(i);
    s = db1->GetExternal(ReadOptions(), k, &value);
    assert(s.ok());
    assert(value == expected_value);
  }

  delete args;
  delete db1;
  DestroyDB(kDBPath, dbOptions);
  unlink(logfile.c_str());
  return 1;
}


int main() {
    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    dbOptions.IncreaseParallelism();
    dbOptions.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    dbOptions.create_if_missing = true;

    // correctness tests
//    assert(testOneDBOneValue() == 1);
    assert(testOneDBMultiValue() == 1);
    // assert(testOneDBMultiValue2() == 1);
    // assert(testOneDBMissingKey() == 1);
    // assert(testTwoDBSharedOneValue() == 1);
    // assert(testTwoDBSharedMultiValue() == 1);
    assert(concurrentWriteTest(1, 100) == 1);
    assert(concurrentWriteTest(20, 1000) == 1);
    // this takes awhile but passes
    //    assert(concurrentWriteTest(8, 1000000) == 1);
    
    return 0;
}
