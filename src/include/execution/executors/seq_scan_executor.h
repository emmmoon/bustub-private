//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "catalog/catalog.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  /** Initialize the sequential scan */
  void Init() override;

  /**
   * Yield the next tuple from the sequential scan.
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sequential scan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sequential scan plan node to be executed */
  const SeqScanPlanNode *plan_;
  const TableInfo *tableinfo_;
  const Catalog *catalog_;
  AbstractExpressionRef predicate_;
  std::shared_ptr<TableIterator> iter_;
  LockManager *lock_mgr_;
  Transaction *txn_;
  bool ctx_is_deleted_;
  table_oid_t oid_;
  bool table_has_locked_ = false;
  bool out_ = false;
};
}  // namespace bustub
