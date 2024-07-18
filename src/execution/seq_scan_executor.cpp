//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "common/exception.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  lock_mgr_ = exec_ctx_->GetLockManager();
  txn_ = exec_ctx_->GetTransaction();
  ctx_is_deleted_ = exec_ctx_->IsDelete();
  oid_ = plan_->GetTableOid();
  out_ = false;
  auto old_lock_mode = lock_mgr_->IsTableLocked(txn_, oid_);
  if (old_lock_mode.has_value()) {
    if (old_lock_mode.value() == LockManager::LockMode::INTENTION_EXCLUSIVE ||
        old_lock_mode.value() == LockManager::LockMode::EXCLUSIVE) {
      table_has_locked_ = true;
    }
  }
  if (!table_has_locked_) {
    if (ctx_is_deleted_) {
      try {
        auto result = lock_mgr_->LockTable(txn_, LockManager::LockMode::INTENTION_EXCLUSIVE, oid_);
        if (!result) {
          throw ExecutionException("X lock table return false. cannot get the table lock");
        }
      } catch (TransactionAbortException &exception) {
        throw ExecutionException(exception.GetInfo());
      }
    } else if (txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
      try {
        auto result = lock_mgr_->LockTable(txn_, LockManager::LockMode::INTENTION_SHARED, oid_);
        if (!result) {
          throw ExecutionException("IS lock table return false. cannot get the table lock");
        }
      } catch (TransactionAbortException &exception) {
        throw ExecutionException(exception.GetInfo());
      }
    }
  }
  catalog_ = exec_ctx_->GetCatalog();
  tableinfo_ = catalog_->GetTable(oid_);
  predicate_ = plan_->filter_predicate_;
  iter_ = std::make_shared<TableIterator>(tableinfo_->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!iter_->IsEnd()) {
    *rid = iter_->GetRID();
    bool row_has_locked = false;
    auto old_lock_mode = lock_mgr_->IsRowLocked(txn_, oid_, *rid);
    if (old_lock_mode.has_value()) {
      if (old_lock_mode.value() == LockManager::LockMode::EXCLUSIVE) {
        row_has_locked = true;
      }
    }
    if (!row_has_locked) {
      if (ctx_is_deleted_) {
        try {
          auto result = lock_mgr_->LockRow(txn_, LockManager::LockMode::EXCLUSIVE, oid_, *rid);
          if (!result) {
            throw ExecutionException("X lock row return false. cannot get the row lock");
          }
        } catch (TransactionAbortException &exception) {
          throw ExecutionException(exception.GetInfo());
        }
      } else if (txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        try {
          auto result = lock_mgr_->LockRow(txn_, LockManager::LockMode::SHARED, oid_, *rid);
          if (!result) {
            throw ExecutionException("S lock row return false. cannot get the row lock");
          }
        } catch (TransactionAbortException &exception) {
          throw ExecutionException(exception.GetInfo());
        }
      }
    }
    auto tuple_pair = iter_->GetTuple();
    if (tuple_pair.first.is_deleted_) {
      if (txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED && !row_has_locked && !ctx_is_deleted_) {
        lock_mgr_->UnlockRow(txn_, oid_, *rid, true);
      }
      ++(*iter_);
      continue;
    }
    *tuple = tuple_pair.second;
    ++(*iter_);
    if (predicate_ == nullptr) {
      if (txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && !row_has_locked && !ctx_is_deleted_) {
        lock_mgr_->UnlockRow(txn_, oid_, *rid);
      }
      return true;
    }
    auto value = predicate_->Evaluate(tuple, GetOutputSchema());
    if (!value.IsNull() && value.GetAs<bool>()) {
      if (txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && !row_has_locked && !ctx_is_deleted_) {
        lock_mgr_->UnlockRow(txn_, oid_, *rid);
      }
      return true;
    }
    if (txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED && !row_has_locked && !ctx_is_deleted_) {
      lock_mgr_->UnlockRow(txn_, oid_, *rid, true);
    }
  }
  if (!out_) {
    if (txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && !table_has_locked_ && !ctx_is_deleted_) {
      lock_mgr_->UnlockTable(txn_, oid_);
    }
    out_ = true;
  }

  return false;
}

}  // namespace bustub
