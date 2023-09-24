//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "common/rid.h"
#include "execution/executors/aggregation_executor.h"
#include "storage/table/tuple.h"
#include "type/type.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.End()) {
  child_ = std::move(child);
  if (plan_->GetGroupBys().empty()) {
    is_group_null_ = true;
  }
}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple t;
  RID r;
  aht_.Clear();
  while (child_->Next(&t, &r)) {
    ++cnt_;
    auto k = MakeAggregateKey(&t);
    auto v = MakeAggregateValue(&t);
    aht_.InsertCombine(k, v);
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cnt_ == 0 && !out_) {
    if (!is_group_null_) {
      return false;
    }
    std::vector<Value> values;
    auto aggregates_types = plan_->GetAggregateTypes();
    for (auto &aggregates_type : aggregates_types) {
      if (aggregates_type == AggregationType::CountStarAggregate) {
        values.emplace_back(ValueFactory::GetIntegerValue(0));
      } else {
        values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
      }
    }
    *tuple = Tuple(values, &GetOutputSchema());
    out_ = true;
    return true;
  }
  while (aht_iterator_ != aht_.End()) {
    auto k = aht_iterator_.Key().group_bys_;
    auto v = aht_iterator_.Val().aggregates_;
    std::vector<Value> values;
    values.reserve(k.size() + v.size());
    for (auto const &x : k) {
      values.push_back(x);
    }
    for (auto const &x : v) {
      values.push_back(x);
    }
    *tuple = Tuple(values, &GetOutputSchema());
    ++aht_iterator_;
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
