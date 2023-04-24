//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (!history_list_.empty()) {
    for (auto it = history_list_.rbegin(); it != history_list_.rend(); ++it) {
      if (entries_[*it].evictable_) {
        *frame_id = *it;
        history_list_.erase(std::next(it).base());
        entries_.erase(*frame_id);
        --curr_size_;
        return true;
      }
    }
  }
  if (!cache_list_.empty()) {
    for (auto it = cache_list_.rbegin(); it != cache_list_.rend(); ++it) {
      if (entries_[*it].evictable_) {
        *frame_id = *it;
        cache_list_.erase(std::next(it).base());
        entries_.erase(*frame_id);
        --curr_size_;
        return true;
      }
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::invalid_argument(("Invalid argument ") + std::to_string(frame_id));
  }
  auto new_size = ++entries_[frame_id].in_count_;
  if (new_size == 1) {
    history_list_.emplace_front(frame_id);
    ++curr_size_;
    entries_[frame_id].pos_ = history_list_.begin();
  } else if (new_size == k_) {
    history_list_.erase(entries_[frame_id].pos_);
    cache_list_.emplace_front(frame_id);
    entries_[frame_id].pos_ = cache_list_.begin();
  } else if (new_size > k_) {
    cache_list_.erase(entries_[frame_id].pos_);
    cache_list_.emplace_front(frame_id);
    entries_[frame_id].pos_ = cache_list_.begin();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::invalid_argument(("Invalid argument ") + std::to_string(frame_id));
  }
  if (entries_.find(frame_id) == entries_.end()) {
    return;
  }
  if (entries_[frame_id].evictable_ && !set_evictable) {
    --curr_size_;
  }
  if (!entries_[frame_id].evictable_ && set_evictable) {
    ++curr_size_;
  }
  entries_[frame_id].evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::invalid_argument(("Invalid argument ") + std::to_string(frame_id));
  }
  if (entries_.find(frame_id) == entries_.end()) {
    return;
  }
  if (entries_[frame_id].in_count_ < k_) {
    history_list_.erase(entries_[frame_id].pos_);
    entries_.erase(frame_id);
    --curr_size_;
  } else {
    cache_list_.erase(entries_[frame_id].pos_);
    entries_.erase(frame_id);
    --curr_size_;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
