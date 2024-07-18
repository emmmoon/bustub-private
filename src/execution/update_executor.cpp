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

#include "concurrency/transaction.h"
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
  auto indexinfos = catalog_->GetTableIndexes(table_info_->name_);
  while (child_executor_->Next(tuple, rid)) {
    std::vector<Value> new_values{};
    for (const auto &expr : plan_->target_expressions_) {
      new_values.push_back(expr->Evaluate(tuple, table_info_->schema_));
    }
    Tuple new_tuple = Tuple(new_values, &table_info_->schema_);
    TupleMeta tmi{INVALID_TXN_ID, INVALID_TXN_ID, false};
    table_info_->table_->UpdateTupleInPlaceUnsafe(tmi, new_tuple, *rid);
    // auto write_record = TableWriteRecord{table_info_->oid_, *rid, table_info_->table_.get()};
    // write_record.wtype_ = WType::UPDATE;
    // exec_ctx_->GetTransaction()->AppendTableWriteRecord(write_record);
    for (auto &indexinfo : indexinfos) {
      auto old_key =
          tuple->KeyFromTuple(table_info_->schema_, indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs());
      const auto new_key =
          new_tuple.KeyFromTuple(table_info_->schema_, indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs());
      indexinfo->index_->DeleteEntry(old_key, *rid, exec_ctx_->GetTransaction());
      indexinfo->index_->InsertEntry(new_key, *rid, exec_ctx_->GetTransaction());
      // exec_ctx_->GetTransaction()->AppendIndexWriteRecord(IndexWriteRecord(*rid, table_info_->oid_, WType::UPDATE,
      // *tuple, indexinfo->index_oid_, exec_ctx_->GetCatalog()));
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
