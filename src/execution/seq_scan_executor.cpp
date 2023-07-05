//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
}

void SeqScanExecutor::Init() {
  // READ_UNCOMMITTED 无需加锁
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      bool is_locked = exec_ctx_->GetLockManager()->LockTable(
          exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, table_info_->oid_);
      if (!is_locked) {
        throw ExecutionException("SeqScan Executor can not get the Table lock!");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException("SeqScan Executor can not get the Table lock!");
    }
  }
  this->table_iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (this->table_iter_ == table_info_->table_->End()) {
    // READ_COMMITTED需要释放锁
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      const auto locked_row_set = exec_ctx_->GetTransaction()->GetSharedRowLockSet()->at(table_info_->oid_);
      table_oid_t oid = table_info_->oid_;
      for (auto t_rid : locked_row_set) {
        exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), oid, t_rid);
      }
      exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), oid);
    }
    return false;
  }
  *tuple = *this->table_iter_;
  *rid = tuple->GetRid();
  ++table_iter_;
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    try {
      bool is_locked = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                            table_info_->oid_, *rid);
      if (!is_locked) {
        throw ExecutionException("SeqScan Executor can not get the Row lock!");
      }
    } catch (TransactionAbortException e) {
      throw ExecutionException("SeqScan Executor can not get the Row lock!");
    }
  }
  return true;
}
}  // namespace bustub
