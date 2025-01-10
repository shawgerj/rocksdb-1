//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <atomic>
#include <memory>
#include <thread>
#include <vector>
#include "db/db_test_util.h"
#include "db/write_batch_internal.h"
#include "db/write_thread.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "test_util/fault_injection_test_env.h"
#include "test_util/sync_point.h"
#include "util/string_util.h"
#include "wotr.h"

namespace rocksdb {

// Test variations of WriteImpl.
class DBWriteTest : public DBTestBase, public testing::WithParamInterface<int> {
 public:
  DBWriteTest() : DBTestBase("/db_write_test") {}

  Options GetOptions() { return DBTestBase::GetOptions(GetParam()); }

  void Open() { DBTestBase::Reopen(GetOptions()); }
};

TEST_P(DBWriteTest, InitAndRegisterWOTR) {
  std::string logfile = "/tmp/wotrlog.txt";
  auto w = std::make_shared<Wotr>(logfile.c_str());
  ASSERT_OK(dbfull()->SetExternal(w.get(), false));
  w->CloseAndDestroy();
}

void to_be_bytes(char* bytearr, size_t num, int pos) {
  for (size_t i = 0; i < sizeof(size_t); i++) {
    bytearr[((pos+1) * 8 - 1) - i] = (num >> (i * 8)) & 0xFF;
  }
}

std::string buildLocator(size_t offset, size_t klen, size_t vlen) {
  struct wotr_ref loc;
  size_t final_offset = offset + klen + sizeof(item_header);
  loc.offset = final_offset;
  loc.len = vlen;
  return std::string(reinterpret_cast<char*>(&loc), sizeof(struct wotr_ref));
}
  
TEST_P(DBWriteTest, SingleWriteWOTR) {
  std::string logfile = "/tmp/wotrlog.txt";
  auto w = std::make_shared<Wotr>(logfile.c_str());
  ASSERT_OK(dbfull()->SetExternal(w.get(), false));

  std::vector<size_t> offsets;
  WriteBatch batch;

  std::string k = "key1";
  std::string v = "value1";
  
  batch.Put(k, v);
  ASSERT_OK(dbfull()->Write(WriteOptions(), &batch, &offsets));
  ASSERT_EQ(offsets.size(), 1);

  PinnableSlice value;
  ASSERT_OK(dbfull()->GetExternal(ReadOptions(), k, &value));
  ASSERT_EQ(value.ToString(), v);

  WriteBatch batch2;
  std::string k_ext = "key1ext";
  std::string locator = buildLocator(offsets[0], k.length(), v.length());
  batch2.Put(k_ext, locator);
  
  ASSERT_OK(dbfull()->Write(WriteOptions(), &batch2, nullptr));

  PinnableSlice value2;
  ASSERT_OK(dbfull()->GetPExternal(ReadOptions(), k_ext, &value2));
  ASSERT_EQ(value2.ToString(), v);
  w->CloseAndDestroy();
}

TEST_P(DBWriteTest, ManyWriteWOTR) {
  int num_writes = 4;
  std::string logfile = "/tmp/wotrlog.txt";
  auto w = std::make_shared<Wotr>(logfile.c_str());
  ASSERT_OK(dbfull()->SetExternal(w.get(), false));

  std::vector<std::string> keys;
  std::vector<std::string> values;
  std::vector<std::string> locators;
  std::vector<std::string> keys_ext;
  for (int i = 0; i < num_writes; i++) {
    std::ostringstream k, v;
    k << "key" << i;
    v << "value" << i;
    keys.push_back(k.str());
    values.push_back(v.str());
  } 

  std::vector<size_t> offsets;
  WriteBatch batch;
  batch.Put(keys[0], values[0]);
  batch.Put(keys[1], values[1]);
  ASSERT_OK(dbfull()->Write(WriteOptions(), &batch, &offsets));

  for (int i = 0; i < 2; i++) {
    std::string loc = buildLocator(offsets[i], keys[i].length(), values[i].length());
    locators.push_back(loc);
  }

  WriteBatch batch2;
  batch2.Put(keys[2], values[2]);
  batch2.Put(keys[3], values[3]);
  ASSERT_OK(dbfull()->Write(WriteOptions(), &batch2, &offsets));

  for (int i = 2; i < num_writes; i++) {
    std::string loc = buildLocator(offsets[i], keys[i].length(), values[i].length());
    locators.push_back(loc);
  }

  PinnableSlice value;
  // get value
  ASSERT_OK(dbfull()->GetExternal(ReadOptions(), keys[2], &value));
  ASSERT_EQ(value.ToString(), values[2]);
  ASSERT_OK(dbfull()->GetExternal(ReadOptions(), keys[3], &value));
  ASSERT_EQ(value.ToString(), values[3]);
  ASSERT_OK(dbfull()->GetExternal(ReadOptions(), keys[0], &value));
  ASSERT_EQ(value.ToString(), values[0]);
  ASSERT_OK(dbfull()->GetExternal(ReadOptions(), keys[1], &value));
  ASSERT_EQ(value.ToString(), values[1]);

  WriteBatch batch_ext;
  for (int i = 0; i < num_writes; i++) {
    std::ostringstream k;
    k << keys[i] << "_ext";
    keys_ext.push_back(k.str());
    batch_ext.Put(k.str(), locators[i]);
  }

  ASSERT_OK(dbfull()->Write(WriteOptions(), &batch_ext, nullptr));

  for (int i = 0; i < num_writes; i++) {
    PinnableSlice val;
    ASSERT_OK(dbfull()->GetPExternal(ReadOptions(), keys_ext[i], &val));
    ASSERT_EQ(val.ToString(), values[i]);
  }
  
  w->CloseAndDestroy();
}

TEST_P(DBWriteTest, DeleteWriteWithWOTR) {
  std::string logfile = "/tmp/wotrlog.txt";
  auto w = std::make_shared<Wotr>(logfile.c_str());
  ASSERT_OK(dbfull()->SetExternal(w.get(), false));

//  std::vector<size_t> offsets;
  WriteBatch batch;
  batch.Put("key1", "value1");
  batch.Put("key2", "value2");
  batch.Delete("key1");
  ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));

  std::string value;
  ReadOptions ropt;
  ASSERT_TRUE(dbfull()->Get(ropt, "key1", &value).IsNotFound());

  WriteBatch batch2;
  std::vector<size_t> offsets;
  batch2.Put("key3", "value3");
  batch2.Put("key4", "value4");
  batch2.Delete("key3");
  ASSERT_OK(dbfull()->Write(WriteOptions(), &batch2, &offsets));

  PinnableSlice pvalue;
  std::string svalue;
  ReadOptions ropt2;
  ASSERT_TRUE(dbfull()->Get(ropt2, "key3", &svalue).IsNotFound());  
  ASSERT_TRUE(dbfull()->GetExternal(ropt2, "key3", &pvalue).IsNotFound());
  w->CloseAndDestroy();
}

TEST_P(DBWriteTest, DeleteAfterWriteWithWOTR) {
  std::string logfile = "/tmp/wotrlog.txt";
  auto w = std::make_shared<Wotr>(logfile.c_str());
  ASSERT_OK(dbfull()->SetExternal(w.get(), false));

//  std::vector<size_t> offsets;
  WriteBatch batch;
  batch.Put("key1", "value1");
  batch.Put("key2", "value2");
  ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));

  std::string value;
  ReadOptions ropt;
  ASSERT_OK(dbfull()->Get(ropt, "key1", &value));
  ASSERT_OK(dbfull()->Get(ropt, "key2", &value));

  WriteBatch batch2;
  std::vector<size_t> offsets;
  batch2.Put("key3", "value3");
  batch2.Put("key4", "value4");
  ASSERT_OK(dbfull()->Write(WriteOptions(), &batch2, &offsets));
  ASSERT_OK(dbfull()->Get(ropt, "key3", &value));
  ASSERT_OK(dbfull()->Get(ropt, "key4", &value));

  WriteBatch batch3;
  batch3.Delete("key3");
  ASSERT_OK(dbfull()->Write(WriteOptions(), &batch3, &offsets));
  ASSERT_OK(dbfull()->Delete(WriteOptions(), "key4"));

  PinnableSlice pvalue;
  std::string svalue;
  ReadOptions ropt2;
  ASSERT_TRUE(dbfull()->Get(ropt2, "key3", &svalue).IsNotFound());  
  ASSERT_TRUE(dbfull()->GetExternal(ropt2, "key3", &pvalue).IsNotFound());
  ASSERT_TRUE(dbfull()->Get(ropt2, "key4", &svalue).IsNotFound());  
  ASSERT_TRUE(dbfull()->GetExternal(ropt2, "key4", &pvalue).IsNotFound());

  w->CloseAndDestroy();
}

TEST_P(DBWriteTest, MultiBatchWOTR) {
  Options options = GetOptions();
  if (!options.enable_multi_thread_write) {
    return;
  }
  constexpr int kNumBatch = 8;
  constexpr int kBatchSize = 16;
  options.write_buffer_size = 1024 * 128;
  Reopen(options);
  std::string logfile = "/tmp/wotrlog.txt";
  auto w = std::make_shared<Wotr>(logfile.c_str());
  ASSERT_OK(dbfull()->SetExternal(w.get(), false));

  
  WriteOptions opt;
  std::vector<WriteBatch> data(kNumBatch);
  std::vector<size_t> offsets;

  std::vector<WriteBatch*> batches;
  for (int i = 0; i < kNumBatch; i++) {
    WriteBatch* batch = &data[i];
    batch->Clear();
    for (int k = 0; k < kBatchSize; k++) {
      batch->Put("key_" + ToString(i) + "_" + ToString(k),
                 "value" + ToString(k));
    }
    batches.push_back(batch);
  }
  ASSERT_OK(dbfull()->MultiBatchWrite(opt, std::move(batches), &offsets));
  ASSERT_EQ(offsets.size(), 128);

  ReadOptions ropt;
  PinnableSlice value;
  for (int i = 0; i < kNumBatch; i++) {
    for (int k = 0; k < kBatchSize; k++) {
      ASSERT_OK(dbfull()->GetExternal(ropt,
                                      "key_" + ToString(i) + "_" + ToString(k),
                                      &value));
      std::string expected_value = "value" + ToString(k);
      ASSERT_EQ(expected_value, value.ToString());
    }
  }
  w->CloseAndDestroy();
}

TEST_P(DBWriteTest, MultiThreadWOTR) {
  Options options = GetOptions();
  if (!options.enable_multi_thread_write) {
    return;
  }
  constexpr int kNumThreads = 4;
  constexpr int kNumWrite = 4;
  constexpr int kNumBatch = 8;
  constexpr int kBatchSize = 16;
  options.write_buffer_size = 1024 * 128;
  Reopen(options);
  std::string logfile = "/tmp/wotrlog.txt";
  auto w = std::make_shared<Wotr>(logfile.c_str());
  ASSERT_OK(dbfull()->SetExternal(w.get(), false));
  
  std::vector<port::Thread> threads;
  // we don't know how the writes will be grouped. So count offsets generated
  // per thread, and ensure it all adds up to 2048. 
  std::atomic<uint32_t> total_offsets(0);
  for (int t = 0; t < kNumThreads; t++) {
    threads.push_back(port::Thread(
        [&](int index) {
          WriteOptions opt;
          std::vector<WriteBatch> data(kNumBatch);
          for (int j = 0; j < kNumWrite; j++) {
            std::vector<WriteBatch*> batches;
            std::vector<size_t> offsets;
            for (int i = 0; i < kNumBatch; i++) {
              WriteBatch* batch = &data[i];
              batch->Clear();
              for (int k = 0; k < kBatchSize; k++) {
                batch->Put("key_" + ToString(index) + "_" + ToString(j) + "_" +
                           ToString(i) + "_" + ToString(k),
                           "value" + ToString(k));
              }
              batches.push_back(batch);
            }
            dbfull()->MultiBatchWrite(opt, std::move(batches), &offsets);
            total_offsets += offsets.size();
          }
        },
        t));
  }
  for (int i = 0; i < kNumThreads; i++) {
    threads[i].join();
  }

  ASSERT_EQ(total_offsets, 2048); // 2048 offsets generated in total

  ReadOptions opt;
  for (int t = 0; t < kNumThreads; t++) {
    PinnableSlice value;
    for (int i = 0; i < kNumWrite; i++) {
      for (int j = 0; j < kNumBatch; j++) {
        for (int k = 0; k < kBatchSize; k++) {
          ASSERT_OK(dbfull()->GetExternal(opt,
                                  "key_" + ToString(t) + "_" + ToString(i) +
                                      "_" + ToString(j) + "_" + ToString(k),
                                  &value));
          std::string expected_value = "value" + ToString(k);
          ASSERT_EQ(expected_value, value.ToString());
        }
      }
    }
  }

  Close();
  w->CloseAndDestroy();
}

  TEST_P(DBWriteTest, WotrRecoverNoFlush) {
    if (GetParam() == DBTestBase::kPipelinedWrite
        || GetParam() == DBTestBase::kDefault) {

      Options options = GetOptions();
      // memtable not flushed on shutdown. disableWAL will lose data
      options.avoid_flush_during_shutdown = true;
      CreateAndReopenWithCF({"one", "two"}, options);

      std::string logfile = "/tmp/wotrlog.txt";
      auto w = std::make_shared<Wotr>(logfile.c_str());
      ASSERT_OK(dbfull()->SetExternal(w.get(), false));

      WriteOptions writeOpt = WriteOptions();
      writeOpt.disableWAL = true;

      WriteBatch batch;
      batch.Put(handles_[1], "foo", "v1");
      batch.Put(handles_[2], "baz", "v5");
      std::vector<size_t> offsets;

      ASSERT_OK(dbfull()->Write(writeOpt, &batch, &offsets));

      ReopenWithColumnFamilies({"default", "one", "two"}, options);
      ASSERT_OK(dbfull()->SetExternal(w.get(), true));
      ssize_t wotr_head = w.get()->Head();
      std::cout << "wotr head is " << wotr_head << std::endl;
  
  
      PinnableSlice value;
      ReadOptions opt;
      ASSERT_OK(dbfull()->GetExternal(opt, handles_[1], "foo", &value));
      ASSERT_EQ("v1", value.ToString());
      ASSERT_OK(dbfull()->GetExternal(opt, handles_[1], "foo", &value));
      ASSERT_EQ("v1", value.ToString());
      ASSERT_OK(dbfull()->GetExternal(opt, handles_[2], "baz", &value));
      ASSERT_EQ("v5", value.ToString());
    }
  }

  TEST_P(DBWriteTest, WotrRecoverFlushMemtableCheckHead) {
    // I only support pipelinedwrite for wotr right now, so let's just restrict this
    if (GetParam() == DBTestBase::kPipelinedWrite
	|| GetParam() == DBTestBase::kDefault) {
      Options options = GetOptions();
      CreateAndReopenWithCF({"pikachu"}, options);
    
      std::string logfile = "/tmp/wotrlog.txt";
      auto w = std::make_shared<Wotr>(logfile.c_str());
      ASSERT_OK(dbfull()->SetExternal(w.get(), false));

      WriteOptions writeOpt = WriteOptions();
      writeOpt.disableWAL = true;

      WriteBatch batch;
      batch.Put("foo", "v1");
      batch.Put("baz", "v5");
      std::vector<size_t> offsets;

      ASSERT_OK(dbfull()->Write(writeOpt, &batch, &offsets));

      ReopenWithColumnFamilies({"default", "pikachu"}, options);
      ASSERT_OK(dbfull()->SetExternal(w.get(), false));
      ssize_t wotr_head = w.get()->Head();
      std::cout << "wotr head is " << wotr_head << std::endl;
      ReadOptions readOpt;
      std::string v;
      // I insert wotr head pointer before memtable is flushed on shutdown
      ASSERT_OK(dbfull()->Get(readOpt, "wotr_ptr", &v));
      size_t found_offset;
      memcpy(&found_offset, v.data(), v.size());
      ASSERT_EQ(wotr_head, found_offset);

      // these should pass since the memtable was flushed
      PinnableSlice value;
      ReadOptions opt;
      ASSERT_OK(dbfull()->GetExternal(opt, "foo", &value));
      ASSERT_EQ("v1", value.ToString());
      ASSERT_OK(dbfull()->GetExternal(opt, "foo", &value));
      ASSERT_EQ("v1", value.ToString());
      ASSERT_OK(dbfull()->GetExternal(opt, "baz", &value));
      ASSERT_EQ("v5", value.ToString());

    }
  }



// shawgerj disabled this test because I broke it
// writes to WOTR may sync without WAL enabled
// It is invalid to do sync write while disabling WAL.
// TEST_P(DBWriteTest, SyncAndDisableWAL) {
//   WriteOptions write_options;
//   write_options.sync = true;
//   write_options.disableWAL = true;
//   ASSERT_TRUE(dbfull()->Put(write_options, "foo", "bar").IsInvalidArgument());
//   WriteBatch batch;
//   ASSERT_OK(batch.Put("foo", "bar"));
//   ASSERT_TRUE(dbfull()->Write(write_options, &batch).IsInvalidArgument());
// }

TEST_P(DBWriteTest, IOErrorOnWALWritePropagateToWriteThreadFollower) {
  constexpr int kNumThreads = 5;
  std::unique_ptr<FaultInjectionTestEnv> mock_env(
      new FaultInjectionTestEnv(env_));
  Options options = GetOptions();
  options.env = mock_env.get();
  Reopen(options);
  std::atomic<int> ready_count{0};
  std::atomic<int> leader_count{0};
  std::vector<port::Thread> threads;
  mock_env->SetFilesystemActive(false);

  // Wait until all threads linked to write threads, to make sure
  // all threads join the same batch group.
  SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::JoinBatchGroup:Wait", [&](void* arg) {
        ready_count++;
        auto* w = reinterpret_cast<WriteThread::Writer*>(arg);
        if (w->state == WriteThread::STATE_GROUP_LEADER) {
          leader_count++;
          while (ready_count < kNumThreads) {
            // busy waiting
          }
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  for (int i = 0; i < kNumThreads; i++) {
    threads.push_back(port::Thread(
        [&](int index) {
          // All threads should fail.
          auto res = Put("key" + ToString(index), "value");
          if (options.manual_wal_flush) {
            ASSERT_TRUE(res.ok());
            // we should see fs error when we do the flush

            // TSAN reports a false alarm for lock-order-inversion but Open and
            // FlushWAL are not run concurrently. Disabling this until TSAN is
            // fixed.
            // res = dbfull()->FlushWAL(false);
            // ASSERT_FALSE(res.ok());
          } else {
            ASSERT_FALSE(res.ok());
          }
        },
        i));
  }
  for (int i = 0; i < kNumThreads; i++) {
    threads[i].join();
  }
  ASSERT_EQ(1, leader_count);
  // Close before mock_env destruct.
  Close();
}

TEST_P(DBWriteTest, ManualWalFlushInEffect) {
  Options options = GetOptions();
  Reopen(options);
  // try the 1st WAL created during open
  ASSERT_TRUE(Put("key" + ToString(0), "value").ok());
  ASSERT_TRUE(options.manual_wal_flush != dbfull()->TEST_WALBufferIsEmpty());
  ASSERT_TRUE(dbfull()->FlushWAL(false).ok());
  ASSERT_TRUE(dbfull()->TEST_WALBufferIsEmpty());
  // try the 2nd wal created during SwitchWAL
  dbfull()->TEST_SwitchWAL();
  ASSERT_TRUE(Put("key" + ToString(0), "value").ok());
  ASSERT_TRUE(options.manual_wal_flush != dbfull()->TEST_WALBufferIsEmpty());
  ASSERT_TRUE(dbfull()->FlushWAL(false).ok());
  ASSERT_TRUE(dbfull()->TEST_WALBufferIsEmpty());
}

TEST_P(DBWriteTest, IOErrorOnWALWriteTriggersReadOnlyMode) {
  std::unique_ptr<FaultInjectionTestEnv> mock_env(
      new FaultInjectionTestEnv(env_));
  Options options = GetOptions();
  options.env = mock_env.get();
  Reopen(options);
  for (int i = 0; i < 2; i++) {
    // Forcibly fail WAL write for the first Put only. Subsequent Puts should
    // fail due to read-only mode
    mock_env->SetFilesystemActive(i != 0);
    auto res = Put("key" + ToString(i), "value");
    // TSAN reports a false alarm for lock-order-inversion but Open and
    // FlushWAL are not run concurrently. Disabling this until TSAN is
    // fixed.
    /*
    if (options.manual_wal_flush && i == 0) {
      // even with manual_wal_flush the 2nd Put should return error because of
      // the read-only mode
      ASSERT_TRUE(res.ok());
      // we should see fs error when we do the flush
      res = dbfull()->FlushWAL(false);
    }
    */
    if (!options.manual_wal_flush) {
      ASSERT_FALSE(res.ok());
    }
  }
  // Close before mock_env destruct.
  Close();
}

TEST_P(DBWriteTest, IOErrorOnSwitchMemtable) {
  Random rnd(301);
  std::unique_ptr<FaultInjectionTestEnv> mock_env(
      new FaultInjectionTestEnv(env_));
  Options options = GetOptions();
  options.env = mock_env.get();
  options.writable_file_max_buffer_size = 4 * 1024 * 1024;
  options.write_buffer_size = 3 * 512 * 1024;
  options.wal_bytes_per_sync = 256 * 1024;
  options.manual_wal_flush = true;
  Reopen(options);
  mock_env->SetFilesystemActive(false, Status::IOError("Not active"));
  Status s;
  for (int i = 0; i < 4 * 512; ++i) {
    s = Put(Key(i), RandomString(&rnd, 1024));
    if (!s.ok()) {
      break;
    }
  }
  ASSERT_EQ(s.severity(), Status::Severity::kFatalError);

  mock_env->SetFilesystemActive(true);
  // Close before mock_env destruct.
  Close();
}

// Test that db->LockWAL() flushes the WAL after locking.
TEST_P(DBWriteTest, LockWalInEffect) {
  Options options = GetOptions();
  Reopen(options);
  // try the 1st WAL created during open
  ASSERT_OK(Put("key" + ToString(0), "value"));
  ASSERT_TRUE(options.manual_wal_flush != dbfull()->TEST_WALBufferIsEmpty());
  ASSERT_OK(dbfull()->LockWAL());
  ASSERT_TRUE(dbfull()->TEST_WALBufferIsEmpty(false));
  ASSERT_OK(dbfull()->UnlockWAL());
  // try the 2nd wal created during SwitchWAL
  dbfull()->TEST_SwitchWAL();
  ASSERT_OK(Put("key" + ToString(0), "value"));
  ASSERT_TRUE(options.manual_wal_flush != dbfull()->TEST_WALBufferIsEmpty());
  ASSERT_OK(dbfull()->LockWAL());
  ASSERT_TRUE(dbfull()->TEST_WALBufferIsEmpty(false));
  ASSERT_OK(dbfull()->UnlockWAL());
}

TEST_P(DBWriteTest, MultiThreadWrite) {
  Options options = GetOptions();
  std::unique_ptr<FaultInjectionTestEnv> mock_env(
      new FaultInjectionTestEnv(env_));
  if (!options.enable_multi_thread_write) {
    return;
  }
  constexpr int kNumThreads = 4;
  constexpr int kNumWrite = 4;
  constexpr int kNumBatch = 8;
  constexpr int kBatchSize = 16;
  options.env = mock_env.get();
  options.write_buffer_size = 1024 * 128;
  Reopen(options);
  std::vector<port::Thread> threads;
  for (int t = 0; t < kNumThreads; t++) {
    threads.push_back(port::Thread(
        [&](int index) {
          WriteOptions opt;
          std::vector<WriteBatch> data(kNumBatch);
          for (int j = 0; j < kNumWrite; j++) {
            std::vector<WriteBatch*> batches;
            for (int i = 0; i < kNumBatch; i++) {
              WriteBatch* batch = &data[i];
              batch->Clear();
              for (int k = 0; k < kBatchSize; k++) {
                batch->Put("key_" + ToString(index) + "_" + ToString(j) + "_" +
                               ToString(i) + "_" + ToString(k),
                           "value" + ToString(k));
              }
              batches.push_back(batch);
            }
            dbfull()->MultiBatchWrite(opt, std::move(batches));
          }
        },
        t));
  }
  for (int i = 0; i < kNumThreads; i++) {
    threads[i].join();
  }
  ReadOptions opt;
  for (int t = 0; t < kNumThreads; t++) {
    std::string value;
    for (int i = 0; i < kNumWrite; i++) {
      for (int j = 0; j < kNumBatch; j++) {
        for (int k = 0; k < kBatchSize; k++) {
          ASSERT_OK(dbfull()->Get(opt,
                                  "key_" + ToString(t) + "_" + ToString(i) +
                                      "_" + ToString(j) + "_" + ToString(k),
                                  &value));
          std::string expected_value = "value" + ToString(k);
          ASSERT_EQ(expected_value, value);
        }
      }
    }
  }

  Close();
}

INSTANTIATE_TEST_CASE_P(DBWriteTestInstance, DBWriteTest,
                        testing::Values(DBTestBase::kDefault,
                                        DBTestBase::kConcurrentWALWrites,
                                        DBTestBase::kPipelinedWrite,
                                        DBTestBase::kMultiThreadWrite));

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
