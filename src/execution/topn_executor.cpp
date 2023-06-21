#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  auto cmp = [this](const Tuple &a, const Tuple &b) {
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
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> pq(cmp);
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    pq.push(child_tuple);
    if (pq.size() > plan_->GetN()) {
      pq.pop();
    }
  }
  while (!pq.empty()) {
    child_tuples_.push(pq.top());
    pq.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (child_tuples_.empty()) {
    return false;
  }
  *tuple = child_tuples_.top();
  *rid = tuple->GetRid();
  child_tuples_.pop();
  return true;
}

}  // namespace bustub
