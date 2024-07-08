#include "execution/executors/topn_executor.h"
#include <algorithm>
#include <iterator>
#include <utility>
#include <vector>
#include "common/rid.h"
#include "storage/table/tuple.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  Tuple t;
  RID r;
  cnt_ = 0;
  child_executor_->Init();
  out_tuples_.clear();
  while (child_executor_->Next(&t, &r)) {
    out_tuples_.push_back(t);
  }
  auto mid = plan_->GetN() > out_tuples_.size() ? out_tuples_.end() : (out_tuples_.begin() + plan_->GetN());
  std::partial_sort(out_tuples_.begin(), mid, out_tuples_.end(), [=](const Tuple &x, const Tuple &y) {
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
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto limit = std::min(plan_->GetN(), out_tuples_.size());
  if (cnt_ < limit) {
    *tuple = out_tuples_[cnt_];
    ++cnt_;
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return cnt_; };

}  // namespace bustub
