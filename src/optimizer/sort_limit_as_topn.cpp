#include "execution/executors/limit_executor.h"
#include "execution/executors/sort_executor.h"
#include "execution/executors/topn_executor.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Limit) {
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    const auto &limit = limit_plan.GetLimit();
    BUSTUB_ENSURE(limit_plan.GetChildren().size() == 1, "Limit should have exactly 1 children.");
    if (optimized_plan->GetChildAt(0)->GetType() == PlanType::Sort) {
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*optimized_plan->GetChildAt(0));
      const auto &order_by = sort_plan.GetOrderBy();
      BUSTUB_ENSURE(sort_plan.GetChildren().size() == 1, "Sort should have exactly 1 children.");
      return std::make_shared<TopNPlanNode>(limit_plan.output_schema_, sort_plan.GetChildAt(0), order_by, limit);
    }
  }
  return optimized_plan;
}

}  // namespace bustub
