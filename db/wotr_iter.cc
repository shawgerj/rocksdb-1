#include "db/db_iter.h"
#include "rocksdb/status.h"
#include "rocksdb/cache.h"
#include "cache/clock_cache.h"
#include "wotr.h"
#include <iostream>
#include <iomanip>

#define WOTR_ITER_CACHE_SIZE (16 * 1024L * 1024L)

struct wotr_ref {
  size_t offset;
  size_t len;
};

namespace rocksdb {
  namespace {
      void deleter(const Slice& /*key*/, void* value) {
	free(reinterpret_cast<char*>(value));
      }
  }
    
class WotrDBIter final: public Iterator {
public:
  WotrDBIter(Iterator* dbiter, Wotr* wotr)
    : dbiter_(dbiter),
      wotr_(wotr),
      valid_(false),
      sequential_(0),
      curr_item_(nullptr),
      s_(Status::OK()) {
    cache_ = NewClockCache(WOTR_ITER_CACHE_SIZE, 4, false);
  }

  ~WotrDBIter() {
    delete dbiter_;
    // cache_ shared_ptr is automaticaly cleaned up
  }

  bool Valid() const override {
    return valid_;
  }
  
  void SeekToFirst() override {
    if (curr_item_ != nullptr) {
      cache_->Release(curr_item_, false);
    }

    dbiter_->SeekToFirst();
    sequential_ = 0;
    load_data();
  }
  
  void SeekToLast() override {
    if (curr_item_ != nullptr) {
      cache_->Release(curr_item_, false);
    }

    dbiter_->SeekToLast();
    sequential_ = 0;
    load_data();
  }

  void Seek(const Slice& target) override {
    if (curr_item_ != nullptr) {
      cache_->Release(curr_item_, false);
    }

    dbiter_->Seek(target);
    sequential_ = 0;
    load_data();
  }

  void SeekForPrev(const Slice& target) override {
    if (curr_item_ != nullptr) {
      cache_->Release(curr_item_, false);
    }

    dbiter_->SeekForPrev(target);
    sequential_ = 0;
    load_data();
  }

  void Next() override {
    if (curr_item_ != nullptr) {
      cache_->Release(curr_item_, false);
    }

    dbiter_->Next();
    sequential_ += 1;
    load_data();
  }

  void Prev() override {
    if (curr_item_ != nullptr) {
      cache_->Release(curr_item_, false);
    }
    
    dbiter_->Prev();
    sequential_ += 1;
    load_data();
  }

  Slice key() const override {
    return dbiter_->key();
  }

  Slice value() const override {
    return Slice(Decode(cache_->Value(curr_item_)), cache_->GetCharge(curr_item_));
  }

  Status status() const override {
    return s_;
  }

private:
  struct wotr_ref {
    size_t offset;
    size_t len;
  };
  
  Iterator* dbiter_;
  Wotr* wotr_;
  std::shared_ptr<Cache> cache_;
  bool valid_;
  size_t sequential_;
  Cache::Handle* curr_item_;
  Status s_;

  static void* Encode(char* d) {
    return reinterpret_cast<void*>(d);
  }

  static char* Decode(void* v) {
    return reinterpret_cast<char*>(v);
  }

  Status load_from_ref(Slice key, size_t offset, size_t len) {
    if ((curr_item_ = cache_->Lookup(key)) && curr_item_ != nullptr) {
      return Status::OK();
    }
    
    char* data;
    if (wotr_->WotrPGet(offset, &data, len) < 0) {
      if (curr_item_ != nullptr) {
	cache_->Release(curr_item_, false);
	curr_item_ = nullptr;
      }
      
      return Status::IOError("get_from_ref error reading from logfile.");
    }

    cache_->Insert(key, Encode(data), len, &deleter, &curr_item_);
    return Status::OK();
  }

  void load_data() {
    Slice dbval = dbiter_->value();

    const struct wotr_ref* loc = reinterpret_cast<const struct wotr_ref*>(dbval.data());

    Status s = load_from_ref(dbiter_->key(), loc->offset, loc->len);
    valid_ = s_.ok() ? true : false;
    s_ = s;
  }
};

  Iterator* NewWotrDBIterator(Iterator* dbiter, Wotr* wotr) {
    WotrDBIter* wotr_iter = new WotrDBIter(dbiter, wotr);
    return wotr_iter;
  }
} // namespace rocksdb
