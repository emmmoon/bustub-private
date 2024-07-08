//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <algorithm>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (CanTxnTakeLock(txn, lock_mode)) {
    auto old_lock_mode = IsTableLocked(txn, oid);
    if (old_lock_mode.has_value()) {
      if (old_lock_mode == lock_mode ||
          (old_lock_mode == LockMode::INTENTION_EXCLUSIVE &&
           (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED)) ||
          (old_lock_mode == LockMode::EXCLUSIVE &&
           (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED))) {
        return true;
      }
      if (CanLockUpgrade(old_lock_mode.value(), lock_mode)) {
        table_lock_map_latch_.lock();
        std::shared_ptr<LockRequestQueue> lrq_p = table_lock_map_[oid];
        std::unique_lock<std::mutex> queue_lock(lrq_p->latch_);
        table_lock_map_latch_.unlock();
        if (lrq_p->upgrading_ != INVALID_TXN_ID) {
          AbortAndThrowException(txn, AbortReason::UPGRADE_CONFLICT);
        }
        auto txn_id = txn->GetTransactionId();
        txn_variant_map_latch_.lock();
        txn_variant_map_[txn_id] = oid;
        txn_variant_map_latch_.unlock();
        auto new_lr = std::make_shared<LockRequest>(txn_id, lock_mode, oid);

        auto lr_iter = std::find_if(lrq_p->request_queue_.begin(), lrq_p->request_queue_.end(),
                                    [txn_id](const auto &lock_req) { return lock_req->txn_id_ == txn_id; });

        BUSTUB_ASSERT((*lr_iter)->granted_ && lr_iter != lrq_p->request_queue_.end(),
                      "Cannot find the granted lock of the txn in request queue");

        lrq_p->upgrading_ = txn_id;
        auto next_lr_iter = std::find_if(lrq_p->request_queue_.begin(), lrq_p->request_queue_.end(),
                                         [](const auto &lock_req) { return !lock_req->granted_; });
        lrq_p->request_queue_.erase(lr_iter);

        auto new_lr_iter = lrq_p->request_queue_.insert(next_lr_iter, new_lr);
        switch (old_lock_mode.value()) {
          case LockMode::SHARED:
            txn->GetSharedTableLockSet()->erase(oid);
            break;
          case LockMode::EXCLUSIVE:
            txn->GetExclusiveTableLockSet()->erase(oid);
            break;
          case LockMode::INTENTION_SHARED:
            txn->GetIntentionSharedTableLockSet()->erase(oid);
            break;
          case LockMode::INTENTION_EXCLUSIVE:
            txn->GetIntentionExclusiveTableLockSet()->erase(oid);
            break;
          case LockMode::SHARED_INTENTION_EXCLUSIVE:
            txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
            break;
        }
        lrq_p->cv_.wait(queue_lock, [&]() {
          return txn->GetState() == TransactionState::ABORTED || lrq_p->CheckCompatibility(new_lr_iter);
        });

        if (txn->GetState() == TransactionState::ABORTED) {
          lrq_p->request_queue_.erase(new_lr_iter);
          lrq_p->cv_.notify_all();
          return false;
        }

        (*new_lr_iter)->granted_ = true;
        lrq_p->upgrading_ = INVALID_TXN_ID;

        switch (lock_mode) {
          case LockMode::SHARED:
            txn->GetSharedTableLockSet()->insert(oid);
            break;
          case LockMode::EXCLUSIVE:
            txn->GetExclusiveTableLockSet()->insert(oid);
            break;
          case LockMode::INTENTION_SHARED:
            txn->GetIntentionSharedTableLockSet()->insert(oid);
            break;
          case LockMode::INTENTION_EXCLUSIVE:
            txn->GetIntentionExclusiveTableLockSet()->insert(oid);
            break;
          case LockMode::SHARED_INTENTION_EXCLUSIVE:
            txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
            break;
        }
        return true;
      }
      AbortAndThrowException(txn, AbortReason::INCOMPATIBLE_UPGRADE);
    } else {
      table_lock_map_latch_.lock();
      std::shared_ptr<LockRequestQueue> lrq_p;
      if (table_lock_map_.find(oid) != table_lock_map_.end()) {
        lrq_p = table_lock_map_[oid];
      } else {
        lrq_p = std::make_shared<LockRequestQueue>();
        table_lock_map_[oid] = lrq_p;
      }
      std::unique_lock<std::mutex> queue_lock(lrq_p->latch_);
      table_lock_map_latch_.unlock();
      txn_variant_map_latch_.lock();
      txn_variant_map_[txn->GetTransactionId()] = oid;
      txn_variant_map_latch_.unlock();
      auto new_lr = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
      auto new_lr_iter = lrq_p->request_queue_.insert(lrq_p->request_queue_.end(), new_lr);

      lrq_p->cv_.wait(queue_lock, [&]() {
        return txn->GetState() == TransactionState::ABORTED || lrq_p->CheckCompatibility(new_lr_iter);
      });

      if (txn->GetState() == TransactionState::ABORTED) {
        lrq_p->request_queue_.erase(new_lr_iter);
        lrq_p->cv_.notify_all();
        return false;
      }

      (*new_lr_iter)->granted_ = true;
      switch (lock_mode) {
        case LockMode::SHARED:
          txn->GetSharedTableLockSet()->insert(oid);
          break;
        case LockMode::EXCLUSIVE:
          txn->GetExclusiveTableLockSet()->insert(oid);
          break;
        case LockMode::INTENTION_SHARED:
          txn->GetIntentionSharedTableLockSet()->insert(oid);
          break;
        case LockMode::INTENTION_EXCLUSIVE:
          txn->GetIntentionExclusiveTableLockSet()->insert(oid);
          break;
        case LockMode::SHARED_INTENTION_EXCLUSIVE:
          txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
          break;
      }
      return true;
    }
  }
  return false;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  auto table_lock_mode = IsTableLocked(txn, oid);
  const auto &row_s_table_map = txn->GetSharedRowLockSet();
  const auto &row_x_table_map = txn->GetExclusiveRowLockSet();
  if ((row_s_table_map->find(oid) != row_s_table_map->end() && !row_s_table_map->at(oid).empty()) ||
      (row_x_table_map->find(oid) != row_x_table_map->end() && !row_x_table_map->at(oid).empty())) {
    AbortAndThrowException(txn, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  if (table_lock_mode.has_value()) {
    table_lock_map_latch_.lock();
    {
      std::shared_ptr<LockRequestQueue> lrq_p = table_lock_map_[oid];
      std::unique_lock<std::mutex> queue_lock(lrq_p->latch_);
      table_lock_map_latch_.unlock();

      auto txn_id = txn->GetTransactionId();
      auto lr_iter = std::find_if(lrq_p->request_queue_.begin(), lrq_p->request_queue_.end(),
                                  [txn_id](const auto &lock_req) { return lock_req->txn_id_ == txn_id; });
      BUSTUB_ASSERT((*lr_iter)->granted_ && lr_iter != lrq_p->request_queue_.end(),
                    "Cannot find the granted lock of the txn in request queue");

      lrq_p->request_queue_.erase(lr_iter);

      lrq_p->cv_.notify_all();
    }
    if (txn->GetState() == TransactionState::GROWING) {
      switch (txn->GetIsolationLevel()) {
        case IsolationLevel::READ_COMMITTED:
        case IsolationLevel::READ_UNCOMMITTED:
          if (table_lock_mode.value() == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
          break;
        case IsolationLevel::REPEATABLE_READ:
          if (table_lock_mode.value() == LockMode::EXCLUSIVE || table_lock_mode.value() == LockMode::SHARED) {
            txn->SetState(TransactionState::SHRINKING);
          }
          break;
      }
    }
    switch (table_lock_mode.value()) {
      case LockMode::SHARED:
        txn->GetSharedTableLockSet()->erase(oid);
        break;
      case LockMode::EXCLUSIVE:
        txn->GetExclusiveTableLockSet()->erase(oid);
        break;
      case LockMode::INTENTION_SHARED:
        txn->GetIntentionSharedTableLockSet()->erase(oid);
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        txn->GetIntentionExclusiveTableLockSet()->erase(oid);
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
        break;
    }
    return true;
  }
  AbortAndThrowException(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED) {
    txn->SetState(TransactionState::ABORTED);
    throw bustub::Exception("Transaction state is not compatible.");
  }
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE ||
      lock_mode == LockMode::INTENTION_SHARED) {
    AbortAndThrowException(txn, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  if (CanTxnTakeLock(txn, lock_mode)) {
    auto old_lock_mode = IsRowLocked(txn, oid, rid);
    if (old_lock_mode.has_value()) {
      if (old_lock_mode == lock_mode || lock_mode == LockMode::SHARED) {
        return true;
      }
      auto table_lock_mode = IsTableLocked(txn, oid);
      if (!table_lock_mode.has_value() ||
          (table_lock_mode.value() != LockMode::INTENTION_EXCLUSIVE && table_lock_mode.value() != LockMode::EXCLUSIVE &&
           table_lock_mode.value() != LockMode::SHARED_INTENTION_EXCLUSIVE)) {
        AbortAndThrowException(txn, AbortReason::TABLE_LOCK_NOT_PRESENT);
      }

      row_lock_map_latch_.lock();
      std::shared_ptr<LockRequestQueue> lrq_p = row_lock_map_[rid];
      std::unique_lock<std::mutex> queue_lock(lrq_p->latch_);
      row_lock_map_latch_.unlock();

      if (lrq_p->upgrading_ != INVALID_TXN_ID) {
        AbortAndThrowException(txn, AbortReason::UPGRADE_CONFLICT);
      }
      auto txn_id = txn->GetTransactionId();
      txn_variant_map_latch_.lock();
      txn_variant_map_[txn_id] = rid;
      txn_variant_map_latch_.unlock();
      auto new_lr = std::make_shared<LockRequest>(txn_id, lock_mode, oid, rid);

      auto lr_iter = std::find_if(lrq_p->request_queue_.begin(), lrq_p->request_queue_.end(),
                                  [txn_id](const auto &lock_req) { return lock_req->txn_id_ == txn_id; });

      BUSTUB_ASSERT(
          (*lr_iter)->granted_ && (*lr_iter)->lock_mode_ == LockMode::SHARED && lr_iter != lrq_p->request_queue_.end(),
          "Cannot find the granted row lock of the txn in request queue");

      lrq_p->upgrading_ = txn_id;
      auto next_lr_iter = std::find_if(lrq_p->request_queue_.begin(), lrq_p->request_queue_.end(),
                                       [](const auto &lock_req) { return !lock_req->granted_; });
      lrq_p->request_queue_.erase(lr_iter);
      auto new_lr_iter = lrq_p->request_queue_.insert(next_lr_iter, new_lr);

      txn->GetSharedRowLockSet()->at(oid).erase(rid);
      txn->GetSharedLockSet()->erase(rid);

      lrq_p->cv_.wait(queue_lock, [&]() {
        return txn->GetState() == TransactionState::ABORTED || lrq_p->CheckCompatibilityOfRow(new_lr_iter);
      });

      if (txn->GetState() == TransactionState::ABORTED) {
        lrq_p->request_queue_.erase(new_lr_iter);
        lrq_p->cv_.notify_all();
        return false;
      }

      (*new_lr_iter)->granted_ = true;
      lrq_p->upgrading_ = INVALID_TXN_ID;

      txn->GetExclusiveRowLockSet()->at(oid).insert(rid);
      txn->GetExclusiveLockSet()->insert(rid);

      return true;
    }
    if (lock_mode == LockMode::SHARED) {
      if (!IsTableLocked(txn, oid).has_value()) {
        AbortAndThrowException(txn, AbortReason::TABLE_LOCK_NOT_PRESENT);
      }
    } else {
      auto table_lock_mode = IsTableLocked(txn, oid);
      if (!table_lock_mode.has_value() ||
          (table_lock_mode.value() != LockMode::INTENTION_EXCLUSIVE && table_lock_mode.value() != LockMode::EXCLUSIVE &&
           table_lock_mode.value() != LockMode::SHARED_INTENTION_EXCLUSIVE)) {
        AbortAndThrowException(txn, AbortReason::TABLE_LOCK_NOT_PRESENT);
      }
    }
    row_lock_map_latch_.lock();
    std::shared_ptr<LockRequestQueue> lrq_p;
    if (row_lock_map_.find(rid) != row_lock_map_.end()) {
      lrq_p = row_lock_map_[rid];
    } else {
      lrq_p = std::make_shared<LockRequestQueue>();
      row_lock_map_[rid] = lrq_p;
    }
    std::unique_lock<std::mutex> queue_lock(lrq_p->latch_);
    row_lock_map_latch_.unlock();
    txn_variant_map_latch_.lock();
    txn_variant_map_[txn->GetTransactionId()] = rid;
    txn_variant_map_latch_.unlock();
    auto new_lr = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
    auto new_lr_iter = lrq_p->request_queue_.insert(lrq_p->request_queue_.end(), new_lr);

    lrq_p->cv_.wait(queue_lock, [&]() {
      return txn->GetState() == TransactionState::ABORTED || lrq_p->CheckCompatibilityOfRow(new_lr_iter);
    });

    if (txn->GetState() == TransactionState::ABORTED) {
      lrq_p->request_queue_.erase(new_lr_iter);
      lrq_p->cv_.notify_all();
      return false;
    }

    (*new_lr_iter)->granted_ = true;
    if (lock_mode == LockMode::SHARED) {
      const auto &shared_row_lock_set = txn->GetSharedRowLockSet();
      if (shared_row_lock_set->find(oid) != shared_row_lock_set->end()) {
        txn->GetSharedRowLockSet()->at(oid).insert(rid);
      } else {
        txn->GetSharedRowLockSet()->emplace(oid, std::unordered_set<RID>{rid});
      }
      txn->GetSharedLockSet()->insert(rid);
    } else {
      const auto &exclusive_row_lock_set = txn->GetExclusiveRowLockSet();
      if (exclusive_row_lock_set->find(oid) != exclusive_row_lock_set->end()) {
        txn->GetExclusiveRowLockSet()->at(oid).insert(rid);
      } else {
        txn->GetExclusiveRowLockSet()->emplace(oid, std::unordered_set<RID>{rid});
      }
      txn->GetExclusiveLockSet()->insert(rid);
    }

    return true;
  }
  return false;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  auto row_lock_mode = IsRowLocked(txn, oid, rid);
  if (row_lock_mode.has_value()) {
    row_lock_map_latch_.lock();
    {
      std::shared_ptr<LockRequestQueue> lrq_p = row_lock_map_[rid];
      std::unique_lock<std::mutex> queue_lock(lrq_p->latch_);
      row_lock_map_latch_.unlock();

      auto txn_id = txn->GetTransactionId();
      auto lr_iter = std::find_if(lrq_p->request_queue_.begin(), lrq_p->request_queue_.end(),
                                  [txn_id](const auto &lock_req) { return lock_req->txn_id_ == txn_id; });
      BUSTUB_ASSERT((*lr_iter)->granted_ && lr_iter != lrq_p->request_queue_.end(),
                    "Cannot find the granted lock of the txn in request queue");
      lrq_p->request_queue_.erase(lr_iter);

      lrq_p->cv_.notify_all();
    }
    if (!force && txn->GetState() == TransactionState::GROWING) {
      switch (txn->GetIsolationLevel()) {
        case IsolationLevel::READ_COMMITTED:
        case IsolationLevel::READ_UNCOMMITTED:
          if (row_lock_mode.value() == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
          break;
        case IsolationLevel::REPEATABLE_READ:
          if (row_lock_mode.value() == LockMode::EXCLUSIVE || row_lock_mode.value() == LockMode::SHARED) {
            txn->SetState(TransactionState::SHRINKING);
          }
          break;
      }
    }
    if (row_lock_mode.value() == LockMode::SHARED) {
      txn->GetSharedRowLockSet()->at(oid).erase(rid);
      txn->GetSharedLockSet()->erase(rid);
    } else {
      txn->GetExclusiveRowLockSet()->at(oid).erase(rid);
      txn->GetExclusiveLockSet()->erase(rid);
    }
    return true;
  }
  AbortAndThrowException(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
}

auto LockManager::CanTxnTakeLock(Transaction *txn, LockMode lock_mode) -> bool {
  auto iso_level = txn->GetIsolationLevel();
  switch (txn->GetState()) {
    case TransactionState::GROWING: {
      if (iso_level == IsolationLevel::READ_UNCOMMITTED &&
          (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
           lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
        // txn->SetState(TransactionState::ABORTED);
        // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        AbortAndThrowException(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      break;
    }
    case TransactionState::SHRINKING: {
      if (iso_level == IsolationLevel::READ_COMMITTED &&
          (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED)) {
        break;
      }
      // txn->SetState(TransactionState::ABORTED);
      // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      AbortAndThrowException(txn, AbortReason::LOCK_ON_SHRINKING);
    }
    case TransactionState::COMMITTED:
    case TransactionState::ABORTED:
      txn->SetState(TransactionState::ABORTED);
      throw bustub::Exception("Transaction state is not compatible.");
  }
  return true;
}

auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  switch (curr_lock_mode) {
    case LockMode::SHARED:
      return requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
    case LockMode::EXCLUSIVE:
      return false;
    case LockMode::INTENTION_SHARED:
      return true;
    case LockMode::INTENTION_EXCLUSIVE:
      return requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return requested_lock_mode == LockMode::EXCLUSIVE;
  }
}

auto LockManager::IsTableLocked(Transaction *txn, const table_oid_t &oid) -> std::optional<LockMode> {
  if (txn->IsTableExclusiveLocked(oid)) {
    return LockMode::EXCLUSIVE;
  }
  if (txn->IsTableIntentionExclusiveLocked(oid)) {
    return LockMode::INTENTION_EXCLUSIVE;
  }
  if (txn->IsTableSharedIntentionExclusiveLocked(oid)) {
    return LockMode::SHARED_INTENTION_EXCLUSIVE;
  }
  if (txn->IsTableSharedLocked(oid)) {
    return LockMode::SHARED;
  }
  if (txn->IsTableIntentionSharedLocked(oid)) {
    return LockMode::INTENTION_SHARED;
  }
  return std::nullopt;
}

auto LockManager::IsRowLocked(Transaction *txn, const table_oid_t &oid, const RID &rid) -> std::optional<LockMode> {
  if (txn->IsRowExclusiveLocked(oid, rid)) {
    return LockMode::EXCLUSIVE;
  }
  if (txn->IsRowSharedLocked(oid, rid)) {
    return LockMode::SHARED;
  }
  return std::nullopt;
}

auto LockManager::LockRequestQueue::CheckCompatibility(
    std::list<std::shared_ptr<LockRequest>>::iterator lock_request_iter) -> bool {
  BUSTUB_ASSERT(lock_request_iter != request_queue_.end(), "lock request iterator cannot be the end of queue");
  ++lock_request_iter;

  auto conflict = [](int mask, std::vector<LockMode> &&lock_modes) -> bool {
    return std::any_of(lock_modes.begin(), lock_modes.end(),
                       [mask](LockMode lm) { return (mask & (1 << static_cast<int>(lm))) != 0; });
  };

  int mask = 0;
  for (auto lr_iter = request_queue_.begin(); lr_iter != lock_request_iter; ++lr_iter) {
    const auto &request = *lr_iter;
    const auto &mode = request->lock_mode_;

    switch (mode) {
      case LockMode::SHARED:
        if (conflict(mask, std::vector<LockMode>{LockMode::INTENTION_EXCLUSIVE, LockMode::SHARED_INTENTION_EXCLUSIVE,
                                                 LockMode::EXCLUSIVE})) {
          return false;
        }
        break;
      case LockMode::EXCLUSIVE:
        if (mask != 0) {
          return false;
        }
        break;
      case LockMode::INTENTION_SHARED:
        if (conflict(mask, std::vector<LockMode>{LockMode::EXCLUSIVE})) {
          return false;
        }
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        if (conflict(mask, std::vector<LockMode>{LockMode::SHARED, LockMode::SHARED_INTENTION_EXCLUSIVE,
                                                 LockMode::EXCLUSIVE})) {
          return false;
        }
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        if (conflict(mask, std::vector<LockMode>{LockMode::SHARED, LockMode::SHARED_INTENTION_EXCLUSIVE,
                                                 LockMode::EXCLUSIVE, LockMode::INTENTION_EXCLUSIVE})) {
          return false;
        }
        break;
    }
    mask |= 1 << static_cast<int>(mode);
  }
  return true;
}

auto LockManager::LockRequestQueue::CheckCompatibilityOfRow(
    std::list<std::shared_ptr<LockRequest>>::iterator lock_request_iter) -> bool {
  BUSTUB_ASSERT(lock_request_iter != request_queue_.end(), "lock request iterator cannot be the end of queue");
  ++lock_request_iter;

  auto has_exclusive = false;
  for (auto lr_iter = request_queue_.begin(); lr_iter != lock_request_iter; ++lr_iter) {
    const auto &request = *lr_iter;
    const auto &mode = request->lock_mode_;

    if (has_exclusive) {
      return false;
    }
    if (mode == LockMode::EXCLUSIVE) {
      has_exclusive = true;
    }
  }
  return true;
}

void LockManager::AbortAndThrowException(Transaction *txn, AbortReason reason) {
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), reason);
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
  // table_lock_map_latch_.lock();
  // for(auto &pair : table_lock_map_) {
  //   pair.second->latch_.lock();

  //   pair.second->latch_.unlock();
  // }
  // table_lock_map_latch_.unlock();
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  assert(t1 != t2);
  auto &vec = waits_for_[t2];
  auto iter = std::lower_bound(vec.begin(), vec.end(), t1);
  if ((iter == vec.end()) || (iter != vec.end() && *iter != t1)) {
    vec.insert(iter, t1);
  }
  auto iter2 = std::lower_bound(waits_.begin(), waits_.end(), t2);
  if ((iter2 == waits_.end()) || (iter2 != waits_.end() && *iter2 != t2)) {
    waits_.insert(iter2, t2);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  assert(t1 != t2);
  auto &vec = waits_for_[t2];
  auto iter = std::lower_bound(vec.begin(), vec.end(), t1);
  if (iter != vec.end() && *iter == t1) {
    vec.erase(iter);
  }
  bool t2_held = !vec.empty();
  if (!t2_held) {
    waits_for_.erase(t2);
  }
  if (!t2_held) {
    auto iter2 = std::lower_bound(waits_.begin(), waits_.end(), t2);
    waits_.erase(iter2);
  }
}

void LockManager::RemoveWaitingEdges(const std::shared_ptr<LockRequestQueue> &lock_request_queue,
                                     txn_id_t waiting_txn_id) {
  std::vector<txn_id_t> granted_txn_ids;
  for (const auto &lock_request : lock_request_queue->request_queue_) {
    if (!lock_request->granted_) {
      break;
    }
    granted_txn_ids.emplace_back(lock_request->txn_id_);
  }
  for (auto granted_txn_id : granted_txn_ids) {
    RemoveEdge(waiting_txn_id, granted_txn_id);
  }
  if (waits_for_.find(waiting_txn_id) != waits_for_.end()) {
    waits_for_.erase(waiting_txn_id);
    auto iter = std::find(waits_.begin(), waits_.end(), waiting_txn_id);
    waits_.erase(iter);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  if (waits_.empty()) {
    return false;
  }
  std::unordered_map<txn_id_t, bool> visited;
  for (auto txn_id : waits_) {
    visited.insert({txn_id, false});
  }
  txn_id_t max_txn_id;
  for (auto held_txn_id : waits_) {
    max_txn_id = held_txn_id;
    bool found = HasCycleDfs(held_txn_id, &max_txn_id, visited);
    if (found) {
      *txn_id = max_txn_id;
      return true;
    }
  }
  return false;
}

auto LockManager::HasCycleDfs(txn_id_t cur_txn_id, txn_id_t *max_txn_id, std::unordered_map<txn_id_t, bool> &visited)
    -> bool {
  auto found = visited.find(cur_txn_id);
  if (found == visited.end()) {
    return false;
  }
  if ((*found).second) {
    return true;
  }
  visited[cur_txn_id] = true;
  for (auto waiting_txn_id : waits_for_[cur_txn_id]) {
    bool children_found = HasCycleDfs(waiting_txn_id, max_txn_id, visited);
    if (children_found) {
      *max_txn_id = std::max(*max_txn_id, waiting_txn_id);
      return true;
    }
  }
  visited[cur_txn_id] = false;
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (const auto &held_txn : waits_for_) {
    for (const auto &waiting_txn_id : held_txn.second) {
      edges.emplace_back(waiting_txn_id, held_txn.first);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      GenerateTableEdges();
      GenerateRowEdges();

      txn_id_t txn_id = INVALID_TXN_ID;
      while (HasCycle(&txn_id)) {
        auto txn = txn_manager_->GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);
        txn_variant_map_latch_.lock();
        if (const auto *p = std::get_if<table_oid_t>(&txn_variant_map_[txn_id])) {
          txn_variant_map_latch_.unlock();
          const auto oid = *p;
          table_lock_map_latch_.lock();
          auto &lock_request_queue = table_lock_map_[oid];
          table_lock_map_latch_.unlock();
          RemoveWaitingEdges(lock_request_queue, txn_id);
          lock_request_queue->cv_.notify_all();
        } else {
          const RID rid = std::get<RID>(txn_variant_map_[txn_id]);
          txn_variant_map_latch_.unlock();
          row_lock_map_latch_.lock();
          auto &lock_request_queue = row_lock_map_[rid];
          row_lock_map_latch_.unlock();
          RemoveWaitingEdges(lock_request_queue, txn_id);
          lock_request_queue->cv_.notify_all();
        }
      }
      waits_for_.clear();
      waits_.clear();
    }
  }
}

void LockManager::GenerateTableEdges() {
  table_lock_map_latch_.lock();
  for (const auto &item : table_lock_map_) {
    std::vector<txn_id_t> granted;
    std::vector<txn_id_t> waiting;
    item.second->latch_.lock();
    for (const auto &lock_request : item.second->request_queue_) {
      if (lock_request->granted_) {
        granted.emplace_back(lock_request->txn_id_);
      } else {
        waiting.emplace_back(lock_request->txn_id_);
      }
    }
    item.second->latch_.unlock();
    for (const auto &wait_txn_id : waiting) {
      for (const auto &granted_txn_id : granted) {
        AddEdge(wait_txn_id, granted_txn_id);
      }
    }
  }
  table_lock_map_latch_.unlock();
}

void LockManager::GenerateRowEdges() {
  row_lock_map_latch_.lock();
  for (const auto &item : row_lock_map_) {
    std::vector<txn_id_t> granted;
    std::vector<txn_id_t> waiting;
    item.second->latch_.lock();
    for (const auto &lock_request : item.second->request_queue_) {
      if (lock_request->granted_) {
        granted.emplace_back(lock_request->txn_id_);
      } else {
        waiting.emplace_back(lock_request->txn_id_);
      }
    }
    item.second->latch_.unlock();
    for (const auto &wait_txn_id : waiting) {
      for (const auto &granted_txn_id : granted) {
        AddEdge(wait_txn_id, granted_txn_id);
      }
    }
  }
  row_lock_map_latch_.unlock();
}

}  // namespace bustub
