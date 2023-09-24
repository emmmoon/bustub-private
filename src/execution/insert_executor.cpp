//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <memory>
#include <vector>

#include "common/config.h"
#include "execution/executors/insert_executor.h"
#include "type/type.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  child_executor_ = std::move(child_executor);
  catalog_ = exec_ctx_->GetCatalog();
  tableinfo_ = catalog_->GetTable(plan_->TableOid());
  indexinfos_ = catalog_->GetTableIndexes(tableinfo_->name_);
}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (out_) {
    return false;
  }
  while (child_executor_->Next(tuple, rid)) {
    TupleMeta tm{INVALID_TXN_ID, INVALID_TXN_ID, false};
    RID ridi = (tableinfo_->table_->InsertTuple(tm, *tuple)).value();
    for (auto &indexinfo : indexinfos_) {
      const auto index_key =
          tuple->KeyFromTuple(tableinfo_->schema_, indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs());
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
