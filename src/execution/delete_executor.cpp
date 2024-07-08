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

#include "concurrency/transaction.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  child_executor_ = std::move(child_executor);
  catalog_ = exec_ctx_->GetCatalog();
  tableinfo_ = catalog_->GetTable(plan_->TableOid());
  txn_ = exec_ctx->GetTransaction();
  indexinfos_ = catalog_->GetTableIndexes(tableinfo_->name_);
}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (out_) {
    return false;
  }
  while (child_executor_->Next(tuple, rid)) {
    TupleMeta tm{INVALID_TXN_ID, txn_->GetTransactionId(), true};
    tableinfo_->table_->UpdateTupleMeta(tm, *rid);
    auto delete_table_record = TableWriteRecord{tableinfo_->oid_, *rid, tableinfo_->table_.get()};
    delete_table_record.wtype_ = WType::DELETE;
    txn_->AppendTableWriteRecord(delete_table_record);
    for (auto &indexinfo : indexinfos_) {
      const auto index_key =
          tuple->KeyFromTuple(tableinfo_->schema_, indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs());
      indexinfo->index_->DeleteEntry(index_key, *rid, exec_ctx_->GetTransaction());
      auto delete_index_record =
          IndexWriteRecord(*rid, tableinfo_->oid_, WType::DELETE, index_key, indexinfo->index_oid_, catalog_);
      txn_->AppendIndexWriteRecord(delete_index_record);
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
