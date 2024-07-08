#include "execution/executors/sort_executor.h"
#include <algorithm>
#include "binder/bound_order_by.h"
#include "common/rid.h"
#include "storage/table/tuple.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  Tuple t;
  RID r;
  child_executor_->Init();
  out_tuple_.clear();
  while (child_executor_->Next(&t, &r)) {
    out_tuple_.push_back(t);
  }
  std::sort(out_tuple_.begin(), out_tuple_.end(), [=](const Tuple &x, const Tuple &y) {
    for (const auto &pair : plan_->order_bys_) {
      auto odtype = pair.first;
      auto expr = pair.second;
      auto vx = expr->Evaluate(&x, child_executor_->GetOutputSchema());
      auto vy = expr->Evaluate(&y, child_executor_->GetOutputSchema());
      if (vx.CompareLessThan(vy) == CmpBool::CmpTrue) {
        return odtype == OrderByType::ASC || odtype == OrderByType::DEFAULT;
      }
      if (vx.CompareGreaterThan(vy) == CmpBool::CmpTrue) {
        return odtype == OrderByType::DESC;
      }
    }
    return true;
  });
  idx_ = 0;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (idx_ < out_tuple_.size()) {
    *tuple = out_tuple_[idx_];
    *rid = tuple->GetRid();
    ++idx_;
    return true;
  }
  return false;
}

}  // namespace bustub
