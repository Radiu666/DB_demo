/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, Page *page, int idx) : buffer_pool_manager_(bpm), page_(page), idx_(idx)  {
    if(page != nullptr) {
        leafpage_ = reinterpret_cast<LeafPage *>(page->GetData());
    } else {
        leafpage_ = nullptr;
    }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
    if (page_ != nullptr) {
        page_->RUnlatch();
        buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
    }
}  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { 
    return leafpage_->GetNextPageId() == INVALID_PAGE_ID && idx_ == leafpage_->GetSize();
 }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return leafpage_->GetItem(idx_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & { 
    if(idx_ >= leafpage_->GetSize() - 1 && leafpage_->GetNextPageId() != INVALID_PAGE_ID) {
        auto next_page = buffer_pool_manager_->FetchPage(leafpage_->GetNextPageId());
        page_->RUnlatch();
        buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
        next_page->RLatch();
        page_ = next_page;
        leafpage_ = reinterpret_cast<LeafPage *>(page_);
        idx_ = 0;
    } else {
        ++idx_;
    }
    return *this;
 }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
