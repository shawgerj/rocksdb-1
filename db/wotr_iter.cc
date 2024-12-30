#include "rocksdb/status.h"
#include "rocksdb/cache.h"
#include "cache/clock_cache.h"
#include "wotr.h"

namespace rocksdb {

class WOTRIter : public Iterator {
public:
  // TODO constructor, deconstructor

  bool Valid() const override {
    return valid_;
  }
  
  void SeekToFirst() override {
    dbiter_.SeekToFirst();
    sequential_ = 0;
    load_data();
  }
  
  void SeekToLast() override {
    dbiter_.SeekToFirst();
    sequential_ = 0;
    load_data();
  }

  void Seek(const Slice& target) override {
    dbiter_->Seek(target);
    sequential_ = 0;
    load_data();
  }

  void SeekForPrev(const Slice& target) override {
    dbiter_->SeekForPrev(target);
    sequential_ = 0;
    load_data();
  }

  void Next() override {
    dbiter_->Next();
    sequential_ += 1;
    load_data();
  }

  void Prev() override {
    dbiter_->Prev();
    sequential_ += 1;
    load_data();
  }

  Slice key() const override {
    return dbiter_->key();
  }

  Slice value() const override {
    return Decode(cache::Value(curr_item_));
  }

  Status status() const override {
    return s_;
  }

private:
  struct wotr_ref {
    size_t offset;
    size_t len;
  };
  
  DBIter* dbiter_;
  Cache* cache_;
  bool valid_;
  size_t sequential_;
  Cache::Handle curr_item_;
  Status s_;

  static void* Encode(Slice s) {
    return reinterpret_cast<void*>(s);
  }

  static Slice Decode(void* v) {
    return static_cast<int>(reinterpret_case<Slice>(v));
  }

  Status load_from_ref(Slice key, struct wotr_ref* ref, Cache::Handle** h) {
    if ((*h = cache_->Lookup(key)) != nullptr) {
      return h;
    }
    
    char* data;
    if (wotr_->WotrPGet(ref->offset, &data, ref->len) < 0) {
      return Status::IOError("get_from_ref error reading from logfile.");
    }

    Slice s(data, ref->len);
    cache_->Insert(key, Encode(s), ref->len, ..., &h); // need deleter fn
    return Status::OK();
  }

  void load_data() {
    Slice dbval = dbiter_->value();
    
    struct wotr_ref* ref = reinterpret_cast<struct wotr_ref*>(dbval.data());
    Status s = load_from_ref(dbiter_->key(), ref, &curr_item_);
    valid_ = s_.ok() ? true : false;
    s_ = s;
  }
};

} // namespace rocksdb
