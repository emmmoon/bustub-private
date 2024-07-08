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
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "execution/executors/insert_executor.h"
#include "type/type.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  lock_mgr_ = exec_ctx->GetLockManager();
  txn_ = exec_ctx->GetTransaction();
  child_executor_ = std::move(child_executor);
  catalog_ = exec_ctx_->GetCatalog();
  tableinfo_ = catalog_->GetTable(plan_->TableOid());
  indexinfos_ = catalog_->GetTableIndexes(tableinfo_->name_);
}

void InsertExecutor::Init() {
  child_executor_->Init();
  try {
    auto result = lock_mgr_->LockTable(txn_, LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->TableOid());
    if (!result) {
      throw ExecutionException("Insert IX lock table return false. cannot get the table lock");
    }
  } catch (const TransactionAbortException &exception) {
    throw ExecutionException(exception.what());
  }
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (out_) {
    return false;
  }
  while (child_executor_->Next(tuple, rid)) {
    TupleMeta tm{txn_->GetTransactionId(), INVALID_TXN_ID, false};
    try {
      RID ridi = (tableinfo_->table_->InsertTuple(tm, *tuple)).value();
      auto insert_table_record = TableWriteRecord{tableinfo_->oid_, ridi, tableinfo_->table_.get()};
      insert_table_record.wtype_ = WType::INSERT;
      txn_->AppendTableWriteRecord(insert_table_record);
      for (auto &indexinfo : indexinfos_) {
        const auto index_key =
            tuple->KeyFromTuple(tableinfo_->schema_, indexinfo->key_schema_, indexinfo->index_->GetKeyAttrs());
        indexinfo->index_->InsertEntry(index_key, ridi, exec_ctx_->GetTransaction());
        auto insert_index_record =
            IndexWriteRecord{ridi, tableinfo_->oid_, WType::INSERT, index_key, indexinfo->index_oid_, catalog_};
        txn_->AppendIndexWriteRecord(insert_index_record);
      }
      ++cnt_;
    } catch (const TransactionAbortException &exception) {
      throw ExecutionException(exception.what());
    }
  }
  out_ = true;
  std::vector<Value> values;
  values.emplace_back(Value(INTEGER, cnt_));
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}

}  // namespace bustub
