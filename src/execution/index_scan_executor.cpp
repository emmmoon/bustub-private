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
#include <iostream>
#include <memory>
#include <ostream>

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  catalog_ = exec_ctx_->GetCatalog();
  index_info_ = catalog_->GetIndex(plan_->GetIndexOid());
  table_info_ = catalog_->GetTable(index_info_->table_name_);
  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
}

void IndexScanExecutor::Init() { iter_ = tree_->GetBeginIterator(); }

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
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
}  // namespace bustub
