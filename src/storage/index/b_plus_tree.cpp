#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  root_page_id_latch_.RLock();
  bool found = false;
  Page *page = GetLeafPage(key, Operation::SEARCH, true, transaction);
  //  获取leaf page后，还有RLatch
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  for (int i = 0; i < leaf_page->GetSize(); i++) {
    if (comparator_(leaf_page->KeyAt(i), key) == 0) {
      result->emplace_back(leaf_page->ValueAt(i));
      found = true;
      break;
    }
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return found;
}

/*
 * 找到叶子结点所在页
 * @return : 页面
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPage(const KeyType &key, Operation op, bool first, Transaction *transaction) -> Page * {
  if (transaction == nullptr && op != Operation::SEARCH) {
    throw std::logic_error("Only make transaction is Null when operation is Search!");
  }
  //  if (!first) {
  //    root_page_id_latch.WLock();
  //    transaction->AddIntoPageSet(nullptr);
  //  }
  page_id_t next_page_id = root_page_id_;
  Page *pre_page = nullptr;
  while (true) {
    Page *page = buffer_pool_manager_->FetchPage(next_page_id);
    auto tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (first) {
      if (tree_page->IsLeafPage() && op != Operation::SEARCH) {
        page->WLatch();
        transaction->AddIntoPageSet(page);
      } else {
        page->RLatch();
      }
      if (pre_page == nullptr) {
        root_page_id_latch_.RUnlock();
      } else {
        pre_page->RUnlatch();
        buffer_pool_manager_->UnpinPage(pre_page->GetPageId(), false);
      }
    } else {
      if (op == Operation::SEARCH) {
        throw std::logic_error("SEARCH 's first should be true!");
      }
      page->WLatch();
      if (IsPageSafe(tree_page, op)) {
        ReleaseWLatches(transaction);
      }
      transaction->AddIntoPageSet(page);
    }
    if (tree_page->IsLeafPage()) {
      if (first && !IsPageSafe(tree_page, op)) {
        ReleaseWLatches(transaction);
        root_page_id_latch_.WLock();
        transaction->AddIntoPageSet(nullptr);
        //        LOG_DEBUG("First but not safe!");
        return GetLeafPage(key, op, false, transaction);
      }
      //      if(op == Operation::DELETE) {
      ////        LOG_DEBUG("Curr key %lld, and is WLatch", key.ToString());
      //      }
      return page;
    }
    auto *internal_page = static_cast<InternalPage *>(tree_page);
    // 扫描到当前的key值，>target_key的返回前一个Key的value，<=target_key的则返回当前key的value。
    // 扫描过程中判断大于target_key的位置，如果key全都小于target_key，则取为最后一个key对应的value。
    next_page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
    for (int i = 1; i < internal_page->GetSize(); i++) {
      if (comparator_(internal_page->KeyAt(i), key) > 0) {
        next_page_id = internal_page->ValueAt(i - 1);
        break;
      }
    }
    //    在释放的锁的时候完成unpin
    //    buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false);
    pre_page = page;
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsPageSafe(BPlusTreePage *tree, Operation op) -> bool {
  if (op == Operation::SEARCH) {
    return true;
  }
  if (op == Operation::INSERT) {
    if (tree->IsLeafPage()) {
      return tree->GetSize() < tree->GetMaxSize() - 1;
    }
    return tree->GetSize() < tree->GetMaxSize();
  }
  if (op == Operation::DELETE) {
    if (tree->IsRootPage()) {
      if (tree->IsLeafPage()) {
        return tree->GetSize() > 1;
      }
      return tree->GetSize() > 2;
    }
    return tree->GetSize() > tree->GetMinSize();
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseWLatches(Transaction *transaction) {
  if (transaction == nullptr) {
    return;
  }
  auto page_set = transaction->GetPageSet();
  while (!page_set->empty()) {
    Page *page = page_set->front();
    page_set->pop_front();
    if (page == nullptr) {
      root_page_id_latch_.WUnlock();
    } else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
  }
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  root_page_id_latch_.RLock();
  //  std::cout << transaction->GetThreadId() << std::endl;
  //  LOG_DEBUG("Key: %lld Insert Begin!!!!!!!!!!", key.ToString());
  // 如果为空创建新的leaf node结点
  if (IsEmpty()) {
    root_page_id_latch_.RUnlock();
    root_page_id_latch_.WLock();
    if (IsEmpty()) {
      Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
      UpdateRootPageId(1);  // 修改root_id时需要调用.
      auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
      leaf_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
      leaf_page->SetKeyValue(0, key, value);
      leaf_page->IncreaseSize(1);
      //      ReleaseWLatches(transaction);
      root_page_id_latch_.WUnlock();
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      //      LOG_DEBUG("Key: %lld is inserted in root.", key.ToString());
      //      std::cout << transaction->GetThreadId() << std::endl;
      //      LOG_DEBUG(" Key: %lld Insert END!!!!!!!!!!!!!", key.ToString());
      return true;
    }
    root_page_id_latch_.WUnlock();
    root_page_id_latch_.RLock();
  }
  // 不为空，首先找到对应的leaf node
  Page *page = GetLeafPage(key, Operation::INSERT, true, transaction);
  //  找完后page还在存在写锁,如果不安全则相应父节点也存在写锁
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  // 查重
  for (int i = 0; i < leaf_page->GetSize(); i++) {
    if (comparator_(leaf_page->KeyAt(i), key) == 0) {
      ReleaseWLatches(transaction);
      //      page->WUnlatch();
      //      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
      //      std::cout << transaction->GetThreadId() << std::endl;
      //      LOG_DEBUG("Key: %lld REPEAT END!!!!!!!!!!!!!", key.ToString());
      return false;
    }
  }
  // 插入，不溢出
  leaf_page->Insert(key, value, comparator_);
  if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {
    //    page->WUnlatch();
    //    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    //    LOG_DEBUG("Key: %lld is inserted in leaf_page.", key.ToString());
    ReleaseWLatches(transaction);
    //    std::cout << transaction->GetThreadId() << std::endl;
    //    LOG_DEBUG("Key: %lld Insert END!!!!!!!!!!!!!", key.ToString());
    return true;
  }
  // 溢出
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  auto *new_leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());
  new_leaf_page->Init(new_page_id, leaf_page->GetParentPageId(), leaf_max_size_);
  new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_leaf_page->GetPageId());
  leaf_page->MoveHalf(new_leaf_page, leaf_page->GetMaxSize() / 2);
  //  if (key.ToString() == 53) {
  //    LOG_DEBUG("Here 53!");
  //    LOG_DEBUG("Page id is %d, Page size is %d", new_leaf_page->GetPageId(), new_leaf_page->GetSize());
  //    LOG_DEBUG("Page last value is %lld", (new_leaf_page->GetItem(1).first).ToString());
  //  }
  // 插入父节点 此时page存在WLatch，在page_set里面
  InsertInParent(leaf_page, new_leaf_page->KeyAt(0), new_leaf_page, transaction);
  //  LOG_DEBUG("Key: %lld already finished inserted the process", key.ToString());
  //  ReleaseWLatches(transaction);
  //  std::cout << transaction->GetThreadId() << std::endl;
  //  LOG_DEBUG("Key: %lld Insert END!!!!!!!!!!!!!", key.ToString());
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(BPlusTreePage *old_page, const KeyType &key, BPlusTreePage *new_page,
                                    Transaction *transaction) {
  // 如果前面页面为根节点，更新根节点，把key挪上去，设置好对应的指针关系。
  if (old_page->IsRootPage()) {
    Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
    auto *new_root_page = reinterpret_cast<InternalPage *>(page->GetData());
    new_root_page->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    new_root_page->SetValueAt(0, old_page->GetPageId());
    new_root_page->SetKeyAt(1, key);
    new_root_page->SetValueAt(1, new_page->GetPageId());
    new_root_page->SetSize(2);
    UpdateRootPageId();
    old_page->SetParentPageId(new_root_page->GetPageId());
    new_page->SetParentPageId(new_root_page->GetPageId());
    buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
    ReleaseWLatches(transaction);
    return;
  }
  // 不为根节点，找到old_page的 parent P，插入key
  page_id_t parent_page_id = old_page->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  auto *parent_inter_page = reinterpret_cast<InternalPage *>(parent_page->GetData());
  parent_inter_page->Insert(key, new_page->GetPageId(), comparator_);
  new_page->SetParentPageId(parent_inter_page->GetPageId());
  // 如果插入后的大小小于等于最大值，插入。
  if (parent_inter_page->GetSize() <= parent_inter_page->GetMaxSize()) {
    buffer_pool_manager_->UnpinPage(parent_inter_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
    ReleaseWLatches(transaction);
    return;
  }
  // 插入后大于最大值需要分裂
  page_id_t new_split_page_id;
  Page *new_split_page = buffer_pool_manager_->NewPage(&new_split_page_id);
  //  if (new_page->GetPageId() == 99) {
  //    LOG_DEBUG("new_split_page_id : %d", new_split_page_id);
  //  }
  auto *new_internal_split_page = reinterpret_cast<InternalPage *>(new_split_page->GetData());
  new_internal_split_page->Init(new_split_page_id, parent_inter_page->GetParentPageId(), internal_max_size_);
  int new_split_page_size = internal_max_size_ / 2 + 1;
  size_t start_index = parent_inter_page->GetSize() - new_split_page_size;
  //  if (new_page->GetPageId() == 99) {
  //    LOG_DEBUG("Here! after insert! before split!");
  //    LOG_DEBUG("parent_inter_page->GetSize(): %d", parent_inter_page->GetSize());
  //  }
  for (int i = start_index, j = 0; i < parent_inter_page->GetSize(); i++, j++) {
    new_internal_split_page->SetKeyAt(j, parent_inter_page->KeyAt(i));
    new_internal_split_page->SetValueAt(j, parent_inter_page->ValueAt(i));
    new_internal_split_page->IncreaseSize(1);
    //    插入过程更改子节点的父指针，该子节点未被加锁。
    Page *page = buffer_pool_manager_->FetchPage(parent_inter_page->ValueAt(i));
    //    page->WLatch();
    auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    //    if (tree_page != nullptr) {
    //      tree_page->SetParentPageId(new_split_page_id);
    //      //    page->WUnlatch();
    //      buffer_pool_manager_->UnpinPage(tree_page->GetPageId(), true);
    //    }
    tree_page->SetParentPageId(new_split_page_id);
    //    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(tree_page->GetPageId(), true);
  }
  parent_inter_page->SetSize(internal_max_size_ - new_split_page_size + 1);
  //  if (key.ToString() == 4) {
  //    LOG_DEBUG("Here!");
  //    LOG_DEBUG("Page id is %d, Page size is %d", parent_inter_page->GetPageId(), parent_inter_page->GetSize());
  //    LOG_DEBUG("Page id is %d, Page size is %d", new_internal_split_page->GetPageId(),
  //    new_internal_split_page->GetSize());
  //    //    LOG_DEBUG("Page last value is %lld", (parent_inter_page->GetItem(3).first).ToString());
  //  }
  //  if (new_page->GetPageId() == 99) {
  //    LOG_DEBUG("Here!");
  //    LOG_DEBUG("parent_inter_page->GetSize(): %d", parent_inter_page->GetSize());
  //        LOG_DEBUG("new_internal_split size is %d, page id is %d, last value is %lld",
  //        new_internal_split_page->GetSize(), new_internal_split_page->GetPageId(),
  //        (new_internal_split_page->KeyAt(1)).ToString());
  //  }
  buffer_pool_manager_->UnpinPage(parent_inter_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
  InsertInParent(parent_inter_page, new_internal_split_page->KeyAt(0), new_internal_split_page, transaction);
  //  old_page WLatch，new_page no
  //  buffer_pool_manager_->UnpinPage(old_page->GetPageId(), true);
  ReleaseWLatches(transaction);
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // 如果为根节点直接删，不为根节点：如果删除后node个数大于min则结束，否则需要向兄弟节点借或者结合
  root_page_id_latch_.RLock();
  if (IsEmpty()) {
    root_page_id_latch_.RUnlock();
    return;
  }
  Page *page = GetLeafPage(key, Operation::DELETE, true, transaction);
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  DeleteEntry(leaf_page, key, transaction);
  auto delete_page_set = transaction->GetDeletedPageSet();
  for (auto &id : *delete_page_set) {
    buffer_pool_manager_->DeletePage(id);
  }
  delete_page_set->clear();
  //  LOG_DEBUG("Delete key : %lld", key.ToString());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteEntry(BPlusTreePage *page, const KeyType &key, Transaction *transaction) {
  // 从page中删除key，分为叶结点以及非叶结点的情况
  //  page存在WLatch
  if (page->IsLeafPage()) {
    auto *leaf_page = static_cast<LeafPage *>(page);
    bool success = leaf_page->Remove(key, comparator_);
    if (success) {
      //      LOG_DEBUG("leaf page has already remove Key %lld", key.ToString());
      //      LOG_DEBUG("leaf page size is %d", leaf_page->GetSize());
    } else {
      ReleaseWLatches(transaction);
      return;
    }
  } else {
    auto *internal_page = static_cast<InternalPage *>(page);
    bool success = internal_page->Remove(key, comparator_);
    if (success) {
      //      LOG_DEBUG("inter page has already remove Key %lld", key.ToString());
    } else {
      ReleaseWLatches(transaction);
      return;
    }
  }
  if (page->IsRootPage() && page->IsLeafPage() && page->GetSize() == 0) {
    transaction->AddIntoDeletedPageSet(page->GetPageId());
    root_page_id_ = INVALID_PAGE_ID;
    ReleaseWLatches(transaction);
    return;
  }
  if (page->IsRootPage() && (page->GetSize() > 1 || page->IsLeafPage())) {
    ReleaseWLatches(transaction);
    return;
  }
  // page为根节点，且删除结点后size==1，即为只剩一下一个指针，把子节点更新为新的根节点。
  if (page->IsRootPage() && page->GetSize() == 1) {
    auto *old_root_page = static_cast<InternalPage *>(page);
    root_page_id_ = old_root_page->ValueAt(0);
    auto *new_root_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
    new_root_page->SetParentPageId(INVALID_PAGE_ID);
    transaction->AddIntoDeletedPageSet(old_root_page->GetPageId());
    ReleaseWLatches(transaction);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    UpdateRootPageId();
    // 删除root page
    return;
  }
  // 小于最小值的情况，合并或者找兄弟结点借。
  if (page->GetSize() < page->GetMinSize()) {
    // 获取兄弟结点
    page_id_t left_peer_id = INVALID_PAGE_ID;
    page_id_t right_peer_id = INVALID_PAGE_ID;
    GetPeerNode(page, &left_peer_id, &right_peer_id);
    if (left_peer_id == INVALID_PAGE_ID && right_peer_id == INVALID_PAGE_ID) {
      throw std::logic_error("Page: " + std::to_string(page->GetPageId()) + "doesn't have peer nodes!");
    }
    Page *l_page = nullptr;
    Page *r_page = nullptr;
    BPlusTreePage *left_peer_page = nullptr;
    BPlusTreePage *right_peer_page = nullptr;
    if (left_peer_id != INVALID_PAGE_ID) {
      l_page = buffer_pool_manager_->FetchPage(left_peer_id);
      l_page->WLatch();
      left_peer_page = reinterpret_cast<BPlusTreePage *>(l_page->GetData());
    }
    if (right_peer_id != INVALID_PAGE_ID) {
      r_page = buffer_pool_manager_->FetchPage(right_peer_id);
      r_page->WLatch();
      right_peer_page = reinterpret_cast<BPlusTreePage *>(r_page->GetData());
    }
    auto *parent_page =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(page->GetParentPageId())->GetData());
    // 又分为4种情况：（左 or 右）*（合并 or 借）
    // 分类：叶子与非叶子
    if (page->IsLeafPage()) {
      LeafPage *selected_merge_page = nullptr;
      LeafPage *selected_borrow_page = nullptr;
      bool is_left = false;
      if (left_peer_page != nullptr && left_peer_page->GetSize() + page->GetSize() < page->GetMaxSize()) {
        selected_merge_page = static_cast<LeafPage *>(left_peer_page);
        is_left = true;
        if (r_page != nullptr) {
          r_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(r_page->GetPageId(), false);
        }
      } else if (right_peer_page != nullptr && right_peer_page->GetSize() + page->GetSize() < page->GetMaxSize()) {
        selected_merge_page = static_cast<LeafPage *>(right_peer_page);
        is_left = false;
        if (l_page != nullptr) {
          l_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(l_page->GetPageId(), false);
        }
      } else if (left_peer_page != nullptr && left_peer_page->GetSize() > page->GetMinSize()) {
        selected_borrow_page = static_cast<LeafPage *>(left_peer_page);
        is_left = true;
        if (r_page != nullptr) {
          r_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(r_page->GetPageId(), false);
        }
      } else if (right_peer_page != nullptr && right_peer_page->GetSize() > page->GetMinSize()) {
        selected_borrow_page = static_cast<LeafPage *>(right_peer_page);
        is_left = false;
        if (l_page != nullptr) {
          l_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(l_page->GetPageId(), false);
        }
      }
      if (selected_borrow_page == nullptr && selected_merge_page == nullptr) {
        throw std::logic_error("Tree structure is wrong!");
      }
      //       合并情况
      if (selected_merge_page) {
        if (is_left) {
          MergePage(selected_merge_page, page, parent_page, transaction);
          l_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(l_page->GetPageId(), true);
        } else {
          // swap(page, selected_merge_page); Value
          MergePage(page, selected_merge_page, parent_page, transaction);
          r_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(r_page->GetPageId(), true);
        }
      }
      // 向兄弟节点借 redistribution
      if (selected_borrow_page) {
        if (is_left) {
          ReDistribution(selected_borrow_page, page, parent_page, is_left, transaction);
          l_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(l_page->GetPageId(), true);
        } else {
          ReDistribution(page, selected_borrow_page, parent_page, is_left, transaction);
          r_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(r_page->GetPageId(), true);
        }
      }
    } else {
      InternalPage *selected_merge_page = nullptr;
      InternalPage *selected_borrow_page = nullptr;
      bool is_left = false;
      if (left_peer_page != nullptr && left_peer_page->GetSize() + page->GetSize() <= page->GetMaxSize()) {
        selected_merge_page = static_cast<InternalPage *>(left_peer_page);
        is_left = true;
        if (r_page != nullptr) {
          r_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(r_page->GetPageId(), false);
        }
      } else if (right_peer_page != nullptr && right_peer_page->GetSize() + page->GetSize() <= page->GetMaxSize()) {
        selected_merge_page = static_cast<InternalPage *>(right_peer_page);
        is_left = false;
        if (l_page != nullptr) {
          l_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(l_page->GetPageId(), false);
        }
      } else if (left_peer_page != nullptr && left_peer_page->GetSize() > page->GetMinSize()) {
        selected_borrow_page = static_cast<InternalPage *>(left_peer_page);
        is_left = true;
        if (r_page != nullptr) {
          r_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(r_page->GetPageId(), false);
        }
      } else if (right_peer_page != nullptr && right_peer_page->GetSize() > page->GetMinSize()) {
        selected_borrow_page = static_cast<InternalPage *>(right_peer_page);
        is_left = false;
        if (l_page != nullptr) {
          l_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(l_page->GetPageId(), false);
        }
      }
      if (selected_borrow_page == nullptr && selected_merge_page == nullptr) {
        throw std::logic_error("Tree structre is wrong!");
      }
      //       合并情况
      if (selected_merge_page) {
        if (is_left) {
          MergePage(selected_merge_page, page, parent_page, transaction);
          l_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(l_page->GetPageId(), true);
        } else {
          // swap(page, selected_merge_page); Value
          MergePage(page, selected_merge_page, parent_page, transaction);
          r_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(r_page->GetPageId(), true);
        }
      }
      // 向兄弟节点借 redistribution
      if (selected_borrow_page) {
        if (is_left) {
          ReDistribution(selected_borrow_page, page, parent_page, is_left, transaction);
          l_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(l_page->GetPageId(), true);
        } else {
          ReDistribution(page, selected_borrow_page, parent_page, is_left, transaction);
          r_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(r_page->GetPageId(), true);
        }
      }
    }
    ReleaseWLatches(transaction);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReDistribution(BPlusTreePage *left, BPlusTreePage *right, InternalPage *parent, bool is_left,
                                    Transaction *transaction) {
  // 原始节点在左右的情况不相同互为镜像，取左最右，取右最左
  auto idx = parent->FindValueIndex(right->GetPageId());
  KeyType key_between = parent->KeyAt(idx);
  if (is_left) {
    // 原始结点在右边，借左边的结点
    if (left->IsLeafPage()) {
      // KeyType key_l;
      // ValueType value_l;
      auto *left_leaf_page = static_cast<LeafPage *>(left);
      auto *right_leaf_page = static_cast<LeafPage *>(right);
      auto key_l = left_leaf_page->KeyAt(left_leaf_page->GetSize() - 1);
      auto value_l = left_leaf_page->ValueAt(left_leaf_page->GetSize() - 1);
      left_leaf_page->DecreaseSize(1);
      right_leaf_page->Insert(key_l, value_l, comparator_);
      parent->SetKeyAt(idx, key_l);
    } else {
      // KeyType key_l;
      // page_id_t value_l;
      auto *left_inter_page = static_cast<InternalPage *>(left);
      auto *right_inter_page = static_cast<InternalPage *>(right);
      auto key_l = left_inter_page->KeyAt(left_inter_page->GetSize() - 1);
      auto value_l = left_inter_page->ValueAt(left_inter_page->GetSize() - 1);
      left_inter_page->DecreaseSize(1);
      right_inter_page->TotalToRight();
      right_inter_page->SetKeyAt(1, key_between);
      right_inter_page->SetValueAt(0, value_l);
      parent->SetKeyAt(idx, key_l);
    }
  } else {
    // 原始结点在左边，借右边的结点，代码镜像
    if (left->IsLeafPage()) {
      // KeyType key_l;
      // ValueType value_l;
      auto *left_leaf_page = static_cast<LeafPage *>(left);
      auto *right_leaf_page = static_cast<LeafPage *>(right);
      auto key_l = right_leaf_page->KeyAt(0);
      auto value_l = right_leaf_page->ValueAt(0);
      right_leaf_page->TotalToLeft();
      left_leaf_page->SetKeyValue(left_leaf_page->GetSize(), key_l, value_l);
      left_leaf_page->IncreaseSize(1);
      parent->SetKeyAt(idx, right_leaf_page->KeyAt(0));
    } else {
      // KeyType key_l;
      // page_id_t value_l;
      auto *left_inter_page = static_cast<InternalPage *>(left);
      auto *right_inter_page = static_cast<InternalPage *>(right);
      auto key_l = right_inter_page->KeyAt(1);
      auto value_l = right_inter_page->ValueAt(0);
      right_inter_page->TotalToLeft();
      left_inter_page->SetKeyAt(left_inter_page->GetSize(), key_between);
      left_inter_page->SetValueAt(left_inter_page->GetSize(), value_l);
      left_inter_page->IncreaseSize(1);
      parent->SetKeyAt(idx, key_l);
    }
  }
  //  buffer_pool_manager_->UnpinPage(left->GetPageId(), true);
  //  buffer_pool_manager_->UnpinPage(right->GetPageId(), true);
  //  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergePage(BPlusTreePage *left, BPlusTreePage *right, InternalPage *parent,
                               Transaction *transaction) {
  // 把right全部挪到left，非叶结点考虑k’。
  auto idx = parent->FindValueIndex(right->GetPageId());
  KeyType key_between = parent->KeyAt(idx);
  if (left->IsLeafPage()) {
    auto *left_leaf_page = static_cast<LeafPage *>(left);
    auto *right_leaf_page = static_cast<LeafPage *>(right);
    right_leaf_page->MoveAll(left_leaf_page, left_leaf_page->GetSize());
    left_leaf_page->SetNextPageId(right_leaf_page->GetNextPageId());
    // parent->Remove(key_between, comparator_);
  } else {
    auto *left_inter_page = static_cast<InternalPage *>(left);
    auto *right_inter_page = static_cast<InternalPage *>(right);
    left_inter_page->SetKeyAt(left_inter_page->GetSize(), key_between);
    left_inter_page->SetValueAt(left_inter_page->GetSize(), right_inter_page->ValueAt(0));
    left_inter_page->IncreaseSize(1);
    right_inter_page->MoveAll(left_inter_page, left_inter_page->GetSize());
  }
  transaction->AddIntoDeletedPageSet(right->GetPageId());
  DeleteEntry(parent, key_between, transaction);
  // 删除已经空的right结点，以及buffer pool更新
  //  buffer_pool_manager_->DeletePage(right->GetPageId());
  //  buffer_pool_manager_->UnpinPage(left->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::GetPeerNode(BPlusTreePage *page, page_id_t *l_peer_id, page_id_t *r_peer_id,
                                 Transaction *transaction) {
  // 获取兄弟结点的page_id
  if (page->IsRootPage()) {
    throw std::logic_error("Root node doesn't have peer nodes!");
  }
  auto *parent_page =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(page->GetParentPageId())->GetData());
  int index = parent_page->FindValueIndex(page->GetPageId());
  if (index == -1) {
    throw std::range_error("Can not find value.");
  }
  *l_peer_id = INVALID_PAGE_ID;
  *r_peer_id = INVALID_PAGE_ID;
  if (index != 0) {
    *l_peer_id = parent_page->ValueAt(index - 1);
  }
  if (index != parent_page->GetSize() - 1) {
    *r_peer_id = parent_page->ValueAt(index + 1);
  }
  //  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  root_page_id_latch_.RLock();
  if (IsEmpty()) {
    root_page_id_latch_.RUnlock();
    return End();
  }
  page_id_t next_page_id = root_page_id_;
  Page *pre_page = nullptr;
  while (true) {
    Page *page = buffer_pool_manager_->FetchPage(next_page_id);
    auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    page->RLatch();
    if (pre_page == nullptr) {
      root_page_id_latch_.RUnlock();
    } else {
      pre_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(pre_page->GetPageId(), false);
    }
    if (tree_page->IsLeafPage()) {
      return INDEXITERATOR_TYPE(buffer_pool_manager_, page, 0);
    }
    auto *inter_page = static_cast<InternalPage *>(tree_page);
    if (inter_page == nullptr) {
      throw std::logic_error("inter_page is null");
    }
    pre_page = page;
    next_page_id = inter_page->ValueAt(0);
    //    buffer_pool_manager_->UnpinPage(inter_page->GetPageId(), false);
  }
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key, Transaction *transaction) -> INDEXITERATOR_TYPE {
  root_page_id_latch_.RLock();
  Page *page = GetLeafPage(key, Operation::SEARCH, true, transaction);
  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  int idx = -1;
  for (int i = 0; i < leaf_page->GetSize(); i++) {
    if (comparator_(leaf_page->KeyAt(i), key) == 0) {
      idx = i;
      break;
    }
  }
  if (idx == -1) {
    throw std::logic_error("can not find such Key in leaf page.");
  }
  //  page->RUnlatch();
  //  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, page, idx);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, nullptr);
  }
  root_page_id_latch_.RLock();
  page_id_t next_page_id = root_page_id_;
  Page *pre_page = nullptr;
  while (true) {
    Page *page = buffer_pool_manager_->FetchPage(next_page_id);
    auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
    page->RLatch();
    if (pre_page == nullptr) {
      root_page_id_latch_.RUnlock();
    } else {
      pre_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(pre_page->GetPageId(), false);
    }
    if (tree_page->IsLeafPage()) {
      return INDEXITERATOR_TYPE(buffer_pool_manager_, page, tree_page->GetSize());
    }
    auto *inter_page = static_cast<InternalPage *>(tree_page);
    if (inter_page == nullptr) {
      throw std::logic_error("inter_page is null");
    }
    pre_page = page;
    next_page_id = inter_page->ValueAt(inter_page->GetSize() - 1);
    //    buffer_pool_manager_->UnpinPage(inter_page->GetPageId(), false);
  }
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
