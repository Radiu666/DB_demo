//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  // you may define your own constructor based on your member variables
  IndexIterator(BufferPoolManager *bpm, Page *page_, int idx = 0);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return page_ == nullptr || (page_->GetPageId() == itr.page_->GetPageId() && idx_ == itr.idx_);
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    return page_->GetPageId() != itr.page_->GetPageId() || idx_ != itr.idx_;
  }

 private:
  // add your own private member variables here
  BufferPoolManager *buffer_pool_manager_;
  Page *page_;
  LeafPage *leafpage_ = nullptr;
  int idx_ = 0;
};

}  // namespace bustub
