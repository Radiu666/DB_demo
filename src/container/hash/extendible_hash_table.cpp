//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size, 0));  // 初始化
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  // 取key的哈希值的后 global_depth_位，例： 2： 100 - 1 = 011，hash & 011 取后两位
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  // 锁定latch_, 并在作用域结束时自动释放锁
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto index = IndexOf(key);
  auto target_bucket = dir_[index];
  return target_bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto index = IndexOf(key);
  auto target_bucket = dir_[index];
  return target_bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  auto target_bucket = dir_[index];
  V val;
  if (target_bucket->Find(key, val)) {
    target_bucket->Insert(key, value);
    return;
  }
  while (dir_[index]->IsFull()) {
    auto target_bucket_2 = dir_[index];
    int local_depth = dir_[index]->GetDepth();
    // 相等：目录翻倍，global_depth++.
    if (GetGlobalDepthInternal() == local_depth) {
      ++global_depth_;
      size_t dir_size = dir_.size();
      dir_.resize(dir_size * 2);
      for (size_t i = 0; i < dir_size; i++) {
        dir_[i + dir_size] = dir_[i];
      }
    }
    auto local_mask = 1 << local_depth;
    auto bucket_0 = std::make_shared<Bucket>(bucket_size_, local_depth + 1);
    auto bucket_1 = std::make_shared<Bucket>(bucket_size_, local_depth + 1);
    for (const auto &it : dir_[index]->GetItems()) {
      auto b_hash_key = std::hash<K>()(it.first);
      if ((b_hash_key & local_mask) == 0) {
        bucket_0->Insert(it.first, it.second);
      } else {
        bucket_1->Insert(it.first, it.second);
      }
    }
    ++num_buckets_;
    for (size_t i = 0; i < dir_.size(); i++) {
      if (dir_[i] == target_bucket_2) {
        if ((i & local_mask) == 0) {
          dir_[i] = bucket_0;
        } else {
          dir_[i] = bucket_1;
        }
      }
    }
    index = IndexOf(key);
  }
  dir_[index]->Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &[k, v] : list_) {
    if (key == k) {
      value = v;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); ++it) {
    if (key == (*it).first) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto &[k, v] : list_) {
    if (key == k) {
      v = value;
      return true;
    }
  }
  if (!IsFull()) {
    list_.emplace_back(key, value);
    return true;
  }
  return false;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
