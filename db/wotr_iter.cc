#include "db/db_iter.h"
#include "rocksdb/status.h"
#include "rocksdb/cache.h"
#include "cache/clock_cache.h"
#include "wotr.h"

#define WOTR_ITER_CACHE_SIZE (16 * 1024 * 1024)

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
      s_(Status::OK()) {
    cache_ = NewClockCache(WOTR_ITER_CACHE_SIZE, 4);
  }

  // TODO deconstructor

  bool Valid() const override {
    return valid_;
  }
  
  void SeekToFirst() override {
    cache_->Release(curr_item_, false);
    dbiter_->SeekToFirst();
    sequential_ = 0;
    load_data();
  }
  
  void SeekToLast() override {
    cache_->Release(curr_item_, false);
    dbiter_->SeekToLast();
    sequential_ = 0;
    load_data();
  }

  void Seek(const Slice& target) override {
    cache_->Release(curr_item_, false);
    dbiter_->Seek(target);
    sequential_ = 0;
    load_data();
  }

  void SeekForPrev(const Slice& target) override {
    cache_->Release(curr_item_, false);
    dbiter_->SeekForPrev(target);
    sequential_ = 0;
    load_data();
  }

  void Next() override {
    cache_->Release(curr_item_, false);
    dbiter_->Next();
    sequential_ += 1;
    load_data();
  }

  void Prev() override {
    cache_->Release(curr_item_, false);
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


  Status load_from_ref(Slice key, const struct wotr_ref* ref, Cache::Handle** h) {
    if ((*h = cache_->Lookup(key)) != nullptr && cache_->Ref(*h)) {
      return Status::OK(); // maybe need to add a ref to h? otherwise no guarantee re. eviction
    }
    
    char* data;
    if (wotr_->WotrPGet(ref->offset, &data, ref->len) < 0) {
      return Status::IOError("get_from_ref error reading from logfile.");
    }

    cache_->Insert(key, Encode(data), ref->len, &deleter, h); // need deleter fn
    return Status::OK();
  }

  void load_data() {
    Slice dbval = dbiter_->value();
    
    const struct wotr_ref* ref = reinterpret_cast<const struct wotr_ref*>(dbval.data());
    Status s = load_from_ref(dbiter_->key(), ref, &curr_item_);
    valid_ = s_.ok() ? true : false;
    s_ = s;
  }
};

  Iterator* NewWotrDBIterator(Iterator* dbiter, Wotr* wotr) {
    WotrDBIter* wotr_iter = new WotrDBIter(dbiter, wotr);
    return wotr_iter;
  }
} // namespace rocksdb
