//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <algorithm>
#include <memory>

#include "execution/executors/update_executor.h"
#include "type/type.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  child_executor_ = std::move(child_executor);
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->TableOid());
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (out_) {
    return false;
  }
  Tuple t;
  RID r;
  auto indexinfos = catalog_->GetTableIndexes(table_info_->name_);
  while (child_executor_->Next(&t, &r)) {
    TupleMeta tmd{INVALID_TXN_ID, INVALID_TXN_ID, true};
    table_info_->table_->UpdateTupleMeta(tmd, r);
    for (auto &indexinfo : indexinfos) {
      const auto index_key =
          t.KeyFromTuple(table_info_->schema_, indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs());
      indexinfo->index_->DeleteEntry(index_key, r, exec_ctx_->GetTransaction());
    }
    std::vector<Value> new_values{};
    for (const auto &expr : plan_->target_expressions_) {
      new_values.push_back(expr->Evaluate(&t, table_info_->schema_));
    }
    Tuple new_tuple = Tuple(new_values, &table_info_->schema_);
    TupleMeta tmi{INVALID_TXN_ID, INVALID_TXN_ID, false};
    RID ridi = (table_info_->table_->InsertTuple(tmi, new_tuple)).value();
    for (auto &indexinfo : indexinfos) {
      const auto index_key =
          new_tuple.KeyFromTuple(table_info_->schema_, indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs());
      indexinfo->index_->InsertEntry(index_key, ridi, exec_ctx_->GetTransaction());
    }
    ++cnt_;
  }
  out_ = true;
  std::vector<Value> values;
  values.emplace_back(Value(INTEGER, cnt_));
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}

}  // namespace bustub
