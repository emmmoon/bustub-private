//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <cstdint>
#include <iostream>
#include <memory>
#include <ostream>
#include <string>
#include <vector>
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "include/storage/index/b_plus_tree_index.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  catalog_ = exec_ctx_->GetCatalog();
  index_info_ = catalog_->GetIndex(plan_->GetIndexOid());
  filter_predicate_ = plan->filter_predicate_;
  table_info_ = catalog_->GetTable(index_info_->table_name_);
  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
  if (!plan_->begin_.empty()) {
    key_schema_ = *plan_->key_schema_;
    convert_from_seq_ = true;
    if (plan_->begin_.size() == 1 &&
        static_cast<bool>(plan_->begin_[0].CompareEquals(plan_->end_[0].Add(ValueFactory::GetIntegerValue(-1))))) {
      single_index_equality_ = true;
    }
  }
}

void IndexScanExecutor::Init() {
  if (!convert_from_seq_) {
    iter_ = tree_->GetBeginIterator();
  }

  if (single_index_equality_) {
    lock_mgr_ = exec_ctx_->GetLockManager();
    txn_ = exec_ctx_->GetTransaction();
    ctx_is_deleted_ = exec_ctx_->IsDelete();
    oid_ = table_info_->oid_;
    auto old_lock_mode = lock_mgr_->IsTableLocked(txn_, oid_);
    if (old_lock_mode.has_value()) {
      if (old_lock_mode.value() == LockManager::LockMode::INTENTION_EXCLUSIVE ||
          old_lock_mode.value() == LockManager::LockMode::EXCLUSIVE) {
        table_has_locked_ = true;
      }
    }
    if (!table_has_locked_) {
      if (txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
        try {
          auto result = lock_mgr_->LockTable(txn_, LockManager::LockMode::INTENTION_SHARED, oid_);
          if (!result) {
            throw ExecutionException("IS lock table return false. cannot get the table lock");
          }
        } catch (TransactionAbortException &exception) {
          throw ExecutionException(exception.GetInfo());
        }
      }
      out_ = false;
    }
    return;
  }

  if (convert_from_seq_) {
    IntegerKeyType begin_key;
    auto begin_key_tuple = Tuple(plan_->begin_, &key_schema_);
    begin_key.SetFromKey(begin_key_tuple);
    iter_ = tree_->GetBeginIterator(begin_key);
    IntegerKeyType end_key;
    auto end_key_tuple = Tuple(plan_->end_, &key_schema_);
    end_key.SetFromKey(end_key_tuple);
    iter_end_ = tree_->GetBeginIterator(end_key);
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!convert_from_seq_) {
    while (!iter_.IsEnd()) {
      auto tuple_pair = table_info_->table_->GetTuple((*iter_).second);
      if (tuple_pair.first.is_deleted_) {
        ++iter_;
        continue;
      }
      *tuple = tuple_pair.second;
      *rid = tuple->GetRid();
      ++iter_;
      return true;
    }
    return false;
  }

  if (single_index_equality_) {
    if (out_) {
      // if(txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      //   lock_mgr_->UnlockRow(txn_, oid_, *rid, true);
      // }
      return false;
    }

    std::vector<RID> rids;
    Tuple key = Tuple(plan_->begin_, index_info_->index_->GetKeySchema());
    index_info_->index_->ScanKey(key, &rids, txn_);
    *rid = rids.back();
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
          if (!table_has_locked_) {
            lock_mgr_->LockTable(txn_, LockManager::LockMode::INTENTION_EXCLUSIVE, oid_);
          }
          auto result_row = lock_mgr_->LockRow(txn_, LockManager::LockMode::EXCLUSIVE, oid_, *rid);
          if (!result_row) {
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
    auto [meta, new_tuple] = table_info_->table_->GetTuple(*rid);
    if (meta.is_deleted_) {
      out_ = true;
      if (txn_->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED && !row_has_locked && !ctx_is_deleted_) {
        lock_mgr_->UnlockRow(txn_, oid_, *rid, true);
      }
      return false;
    }
    if (txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && !row_has_locked && !ctx_is_deleted_) {
      lock_mgr_->UnlockRow(txn_, oid_, *rid);
    }
    *tuple = new_tuple;
    out_ = true;
    return true;
  }

  while (iter_ != iter_end_) {
    auto tuple_pair = table_info_->table_->GetTuple((*iter_).second);
    if (tuple_pair.first.is_deleted_) {
      ++iter_;
      continue;
    }
    *tuple = tuple_pair.second;
    *rid = tuple->GetRid();
    ++iter_;
    if (filter_predicate_ == nullptr) {
      return true;
    }
    auto value = filter_predicate_->Evaluate(tuple, GetOutputSchema());
    if (!value.IsNull() && value.GetAs<bool>()) {
      return true;
    }
  }
  return false;
}
}  // namespace bustub
