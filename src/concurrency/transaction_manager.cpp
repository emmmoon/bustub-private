//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  auto table_write_set = txn->GetWriteSet();
  for (const auto &write_record : *table_write_set) {
    auto old_tm = write_record.table_heap_->GetTupleMeta(write_record.rid_);
    write_record.table_heap_->UpdateTupleMeta(
        TupleMeta{old_tm.insert_txn_id_, old_tm.delete_txn_id_, !old_tm.is_deleted_}, write_record.rid_);
  }

  auto index_write_set = txn->GetIndexWriteSet();
  for (const auto &write_record : *index_write_set) {
    auto index_info = write_record.catalog_->GetIndex(write_record.table_oid_);
    switch (write_record.wtype_) {
      case WType::INSERT:
        index_info->index_->DeleteEntry(write_record.tuple_, write_record.rid_, txn);
        break;
      case WType::DELETE:
        index_info->index_->InsertEntry(write_record.tuple_, write_record.rid_, txn);
        break;
      default:
        break;
    }
  }

  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
