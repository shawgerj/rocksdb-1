// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <iostream>
#include <string>
#include <thread>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "boulevardier.h"

using namespace rocksdb;

std::string rootDir = "/tmp";
std::string kDBPath = "/tmp/rocksdb_simple_example";
Options dbOptions;

int testOneDBOneValue() {
    DB* db;
    Status s = DB::Open(dbOptions, kDBPath, &db);
    
    std::string logfile = rootDir + "/vlog1.txt";
    auto blvd = std::make_shared<Boulevardier>(logfile.c_str());
    db->SetBoulevardier(blvd.get());

    // Put key-value
    size_t o1;
    s = db->PutExternal(WriteOptions(), "key1", "value1", &o1);
    assert(s.ok());

    std::string value;
    // get value
    s = db->GetExternal(ReadOptions(), "key1", &value);
    assert(s.ok());
    assert(value == "value1");

    delete db;

    return 1;
}


int main() {
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  dbOptions.IncreaseParallelism();
  dbOptions.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  dbOptions.create_if_missing = true;

  assert(testOneDBOneValue() == 1);

  return 0;
}
