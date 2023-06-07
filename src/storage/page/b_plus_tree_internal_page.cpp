//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}

/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key{array_[index].first};
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  int size = GetSize();
  if (size > GetMaxSize()) {
    throw std::logic_error("Internal node has already reached over maxsize, which should be less than or equal to Maxsize.");
  }
  // 如果不违规，查找插入的位置，找到第一个大于key的位置，即为插入位置，后面元素向后挪。
  int idx = 1;  // internal 要从1开始
  while(idx < size && comparator(KeyAt(idx), key) < 0) {
    ++idx;
  }
  for (int i = size; i > idx; i--) {
    array_[i] = array_[i - 1];
  }
  SetKeyAt(idx, key);
  SetValueAt(idx, value);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) -> bool {
  int size = GetSize();
  int idx = 1;
  while(idx < size && comparator(KeyAt(idx), key) != 0) {
    ++idx;
  }
  if(comparator(KeyAt(idx), key) != 0) {
//    throw std::range_error("Can not find the key!");
    return false;
  }
  while(idx < size - 1) {
    array_[idx] = array_[idx + 1];
    ++idx;
  }
  DecreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::TotalToLeft() {
  for(int i = 0; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  DecreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::TotalToRight() {
  for(int i = GetSize(); i > 0; i--) {
    array_[i] = array_[i - 1];
  }
  IncreaseSize(1);
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { 
  return array_[index].second;
 }

INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindValueIndex(const ValueType &value) {
  for(int i = 0; i < GetSize(); i++) {
    if(array_[i].second == value) {
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAll(BPlusTreeInternalPage *new_page, int idx) {
  int j = idx;
  for(int i = 1; i < GetSize(); i++) {
    // new_page->SetKeyValue(j++, KeyAt(i), ValueAt(i));
    new_page->SetKeyAt(j, KeyAt(i));
    new_page->SetValueAt(j, ValueAt(i));
    new_page->IncreaseSize(1);
    ++j;
  }
  SetSize(0);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
