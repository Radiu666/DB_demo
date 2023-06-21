//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple;
  RID rid;
  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples_.emplace_back(tuple);
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID e_rid;
  while (right_idx_ >= 0 || left_executor_->Next(&left_tuple_, &e_rid)) {
    std::vector<Value> vals;
    for (uint64_t i = (right_idx_ < 0 ? 0 : right_idx_); i < right_tuples_.size(); i++) {
      auto &right_tuple = right_tuples_[i];
      auto value_join = plan_->Predicate().EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                                        right_executor_->GetOutputSchema());
      if (!value_join.IsNull() && value_join.GetAs<bool>()) {
        for (uint64_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
          vals.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), idx));
        }
        for (uint64_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
          vals.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), idx));
        }
        *tuple = Tuple(vals, &GetOutputSchema());
        right_idx_ = i + 1;
        return true;
      }
    }
    if (right_idx_ == -1 && plan_->GetJoinType() == JoinType::LEFT) {
      for (uint64_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); idx++) {
        vals.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), idx));
      }
      for (uint64_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); idx++) {
        vals.emplace_back(
            ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(idx).GetType()));
      }
      *tuple = Tuple(vals, &GetOutputSchema());
      return true;
    }
    right_idx_ = -1;
  }
  return false;
}

}  // namespace bustub
