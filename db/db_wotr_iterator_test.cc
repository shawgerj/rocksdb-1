#include <atomic>
#include <memory>
#include <thread>
#include <vector>
#include "db/db_test_util.h"
#include "wotr.h"

namespace rocksdb {

class DBWotrIteratorTest : public DBTestBase, public testing::WithParamInterface<int> {
 public:
  DBWotrIteratorTest() : DBTestBase("/db_wotr_iterator_test") {}

  Options GetOptions() { return DBTestBase::GetOptions(GetParam()); }

  void Open() { DBTestBase::Reopen(GetOptions()); }
};

TEST_P(DBWotrIteratorTest, InitAndRegisterWotr) {
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

void buildLocator(std::string* loc, size_t offset, size_t klen, size_t vlen) {
  char arr[16];
  size_t final_offset = offset + klen + sizeof(item_header);
  to_be_bytes(arr, final_offset, 0);
  to_be_bytes(arr, vlen, 1);
  loc->assign(arr, arr + 16);
}


TEST_P(DBWotrIteratorTest, ManyWrites) {
  size_t num_writes = 10;
  std::string logfile = "/tmp/wotrlog.txt";
  auto w = std::make_shared<Wotr>(logfile.c_str());
  ASSERT_OK(dbfull()->SetExternal(w.get(), false));

  std::vector<std::string> keys;
  std::vector<std::string> values;
  std::vector<std::string> locators;
  std::vector<std::string> keys_ext;

  // create all the keys and values
  for (size_t i = 0; i < num_writes; i++) {
    std::ostringstream k, v;
    k << "key" << i;
    v << "value" << i;
    keys.push_back(k.str());
    values.push_back(v.str());
  } 

  // load the db
  std::vector<size_t> offsets;
  for (size_t i = 0; i < keys.size(); i++) {
    // put the key value pair in rocksdb+wotr
    WriteBatch batch;
    WriteBatch batch_ext;
    batch.Put(keys[i], values[i]);
    ASSERT_OK(dbfull()->Write(WriteOptions(), &batch, &offsets));

    // build a locator to read just the value from wotr
    std::string loc;
    buildLocator(&loc, offsets[0], keys[i].length(), values[i].length());

    // put the locator in (only) rocksdb
    // eventually we iterate over all the "iter_*" keys
    std::ostringstream k;
    k << "iter_" << keys[i];
    keys_ext.push_back(k.str());
    batch_ext.Put(k.str(), loc);
    ASSERT_OK(dbfull()->Write(WriteOptions(), &batch_ext, nullptr));
  }

  // read all the values back from rocksdb
  for (size_t i = 0; i < keys.size(); i++) {
    PinnableSlice value;
    ASSERT_OK(dbfull()->GetExternal(ReadOptions(), keys[i], &value));
    ASSERT_EQ(value.ToString(), values[i]);
  }

  // read all the values back using db wotr iterator
  Iterator* iter = dbfull()->NewWotrIterator(ReadOptions());
  iter->Seek(keys_ext[0]);

  for (size_t i = 0; i < keys_ext.size(); i++) {
    ASSERT_OK(iter->status());
    ASSERT_EQ(keys_ext[0], iter->key().ToString());
    ASSERT_EQ(values[0], iter->value().ToString());
    iter->Next();
  }
  
  w->CloseAndDestroy();
}

} // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
