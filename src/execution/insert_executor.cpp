//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
}

void InsertExecutor::Init() {
  child_executor_->Init();
  this->index_infos_ = this->exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_finish_) {
    return false;
  }
  Tuple to_insert_tuple{};
  RID to_rid{};
  int count = 0;
  while (child_executor_->Next(&to_insert_tuple, &to_rid)) {
    bool success = table_info_->table_->InsertTuple(to_insert_tuple, &to_rid, exec_ctx_->GetTransaction());
    if (success) {
      for (auto info : index_infos_) {
        const auto index_key =
            to_insert_tuple.KeyFromTuple(table_info_->schema_, info->key_schema_, info->index_->GetKeyAttrs());
        info->index_->InsertEntry(index_key, to_rid, exec_ctx_->GetTransaction());
      }
      count++;
    }
  }
  std::vector<Value> values{};
  values.reserve(GetOutputSchema().GetColumnCount());
  values.emplace_back(TypeId::INTEGER, count);
  *tuple = Tuple{values, &GetOutputSchema()};
  is_finish_ = true;
  return true;
}
}  // namespace bustub
