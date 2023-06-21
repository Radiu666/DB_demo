#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID child_rid;
  while (child_executor_->Next(&tuple, &child_rid)) {
    child_tuples_.emplace_back(tuple);
  }
  std::sort(child_tuples_.begin(), child_tuples_.end(), [this](const Tuple &a, const Tuple &b) {
    for (auto [order_type, expr] : plan_->GetOrderBy()) {
      switch (order_type) {
        case OrderByType::INVALID:
        case OrderByType::DEFAULT:
        case OrderByType::ASC:
          if (static_cast<bool>(expr->Evaluate(&a, child_executor_->GetOutputSchema())
                                    .CompareLessThan(expr->Evaluate(&b, child_executor_->GetOutputSchema())))) {
            return true;
          } else if (static_cast<bool>(
                         expr->Evaluate(&a, child_executor_->GetOutputSchema())
                             .CompareGreaterThan(expr->Evaluate(&b, child_executor_->GetOutputSchema())))) {
            return false;
          }
          break;
        case OrderByType::DESC:
          if (static_cast<bool>(expr->Evaluate(&a, child_executor_->GetOutputSchema())
                                    .CompareGreaterThan(expr->Evaluate(&b, child_executor_->GetOutputSchema())))) {
            return true;
          } else if (static_cast<bool>(expr->Evaluate(&a, child_executor_->GetOutputSchema())
                                           .CompareLessThan(expr->Evaluate(&b, child_executor_->GetOutputSchema())))) {
            return false;
          }
          break;
      }
    }
    return false;
  });
  child_iter_ = child_tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (child_iter_ == child_tuples_.end()) {
    return false;
  }
  *tuple = *child_iter_;
  *rid = tuple->GetRid();
  ++child_iter_;
  return true;
}

}  // namespace bustub
