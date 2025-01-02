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

std::string buildLocator(size_t offset, size_t klen, size_t vlen) {
  struct wotr_ref loc;
  size_t final_offset = offset + klen + sizeof(item_header);
  loc.offset = final_offset;
  loc.len = vlen;
  return std::string(reinterpret_cast<char*>(&loc), sizeof(struct wotr_ref));
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
  for (size_t i = 0; i < keys.size(); i++) {
    size_t offset;
    
    // put the key value pair in rocksdb+wotr
    ASSERT_OK(dbfull()->PutExternal(WriteOptions(), keys[i], values[i], &offset));

    // build a locator to read just the value from wotr
    std::string loc = buildLocator(offset, keys[i].length(), values[i].length());

    // put the locator in (only) rocksdb
    // eventually we iterate over all the "iter_*" keys
    std::ostringstream k;
    k << "iter_" << keys[i];
    keys_ext.push_back(k.str());
    ASSERT_OK(dbfull()->Put(WriteOptions(), k.str(), loc));
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

  size_t i = 0;
  while (iter->status().ok() && i < keys_ext.size()) {
    ASSERT_EQ(keys_ext[i], iter->key().ToString());
    ASSERT_EQ(values[i], iter->value().ToString());
    iter->Next();
    i++;
  }

  delete iter;
  w->CloseAndDestroy();
}

INSTANTIATE_TEST_CASE_P(DBWotrIteratorTestInstance, DBWotrIteratorTest,
                        testing::Values(DBTestBase::kDefault,
					DBTestBase::kPipelinedWrite));
  
} // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
