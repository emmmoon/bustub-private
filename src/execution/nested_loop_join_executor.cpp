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
#include <cmath>
#include <iostream>
#include <iterator>
#include <ostream>
#include <utility>
#include "binder/table_ref/bound_join_ref.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value.h"
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
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
  }
}

void NestedLoopJoinExecutor::Init() {
  RID r;
  left_executor_->Init();
  right_executor_->Init();
  left_tuple_ = new Tuple();
  right_tuple_ = new Tuple();
  left_executor_->Next(left_tuple_, &r);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID r;
  while (true) {
    if (right_executor_->Next(right_tuple_, &r)) {
      auto value = plan_->predicate_->EvaluateJoin(left_tuple_, left_executor_->GetOutputSchema(), right_tuple_,
                                                   right_executor_->GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        std::vector<Value> values;
        for (uint32_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); ++idx) {
          values.push_back(left_tuple_->GetValue(&left_executor_->GetOutputSchema(), idx));
        }
        for (uint32_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); ++idx) {
          values.push_back(right_tuple_->GetValue(&right_executor_->GetOutputSchema(), idx));
        }
        *tuple = Tuple(values, &GetOutputSchema());
        joined_ = true;
        return true;
      }
    } else {
      if (!joined_ && plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> values;
        for (uint32_t idx = 0; idx < left_executor_->GetOutputSchema().GetColumnCount(); ++idx) {
          values.push_back(left_tuple_->GetValue(&left_executor_->GetOutputSchema(), idx));
        }
        for (uint32_t idx = 0; idx < right_executor_->GetOutputSchema().GetColumnCount(); ++idx) {
          values.push_back(
              ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(idx).GetType()));
        }
        *tuple = Tuple(values, &GetOutputSchema());
        joined_ = true;
        return true;
      }
      if (!left_executor_->Next(left_tuple_, &r)) {
        if (left_tuple_ != nullptr || left_tuple_ != nullptr) {
          delete left_tuple_;
          delete right_tuple_;
          left_tuple_ = nullptr;
          right_tuple_ = nullptr;
        }
        return false;
      }
      right_executor_->Init();
      joined_ = false;
    }
  }
  return true;
}

}  // namespace bustub
