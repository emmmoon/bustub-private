//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <utility>
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  Tuple t;
  RID r;
  jht_.clear();
  left_child_->Init();
  right_child_->Init();
  while (right_child_->Next(&t, &r)) {
    auto jk = MakeRightJoinKey(&t);
    // std::cout << "right table key: "<< jk.joins_.front().ToString() << std::endl;
    jht_.insert({jk, t});
  }
  left_tuple_ = new Tuple();
  left_child_->Next(left_tuple_, &r);
  auto jk = MakeLeftJoinKey(left_tuple_);
  // std::cout << "left table key: "<< jk.joins_.front().ToString() << std::endl;
  auto it_pair = jht_.equal_range(jk);
  jht_cur_iterator_ = it_pair.first;
  jht_end_iterator_ = it_pair.second;
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  RID r;
  while (true) {
    if (jht_cur_iterator_ != jht_end_iterator_) {
      auto right_tuple = jht_cur_iterator_->second;
      std::vector<Value> values;
      for (uint32_t idx = 0; idx < left_child_->GetOutputSchema().GetColumnCount(); ++idx) {
        values.push_back(left_tuple_->GetValue(&left_child_->GetOutputSchema(), idx));
      }
      for (uint32_t idx = 0; idx < right_child_->GetOutputSchema().GetColumnCount(); ++idx) {
        values.push_back(right_tuple.GetValue(&right_child_->GetOutputSchema(), idx));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      joined_ = true;
      ++jht_cur_iterator_;
      return true;
    }
    if (!joined_ && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      for (uint32_t idx = 0; idx < left_child_->GetOutputSchema().GetColumnCount(); ++idx) {
        values.push_back(left_tuple_->GetValue(&left_child_->GetOutputSchema(), idx));
      }
      for (uint32_t idx = 0; idx < right_child_->GetOutputSchema().GetColumnCount(); ++idx) {
        values.push_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(idx).GetType()));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      joined_ = true;
      return true;
    }
    if (left_child_->Next(left_tuple_, &r)) {
      auto jk = MakeLeftJoinKey(left_tuple_);
      // std::cout << "left table key: "<< jk.joins_.front().ToString() << std::endl;
      auto it_pair = jht_.equal_range(jk);
      jht_cur_iterator_ = it_pair.first;
      jht_end_iterator_ = it_pair.second;
      joined_ = false;
    } else {
      if (left_tuple_ != nullptr) {
        delete left_tuple_;
        left_tuple_ = nullptr;
      }
      return false;
    }
  }
  return true;
}

}  // namespace bustub
