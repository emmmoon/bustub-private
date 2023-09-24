//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <utility>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  child_executor_ = std::move(child_executor);
  catalog_ = exec_ctx_->GetCatalog();
  tableinfo_ = catalog_->GetTable(plan_->TableOid());
}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (out_) {
    return false;
  }
  auto indexinfos = catalog_->GetTableIndexes(tableinfo_->name_);
  while (child_executor_->Next(tuple, rid)) {
    TupleMeta tm{INVALID_TXN_ID, INVALID_TXN_ID, true};
    tableinfo_->table_->UpdateTupleMeta(tm, *rid);
    for (auto &indexinfo : indexinfos) {
      const auto index_key =
          tuple->KeyFromTuple(tableinfo_->schema_, indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs());
      indexinfo->index_->DeleteEntry(index_key, *rid, exec_ctx_->GetTransaction());
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
