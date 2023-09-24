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
#include <memory>

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto oid = plan_->GetTableOid();
  catalog_ = exec_ctx_->GetCatalog();
  tableinfo_ = catalog_->GetTable(oid);
  iter_ = std::make_shared<TableIterator>(tableinfo_->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!iter_->IsEnd()) {
    auto tuple_pair = iter_->GetTuple();
    if (tuple_pair.first.is_deleted_) {
      ++(*iter_);
      continue;
    }
    *tuple = tuple_pair.second;
    *rid = iter_->GetRID();
    ++(*iter_);
    return true;
  }
  return false;
}

}  // namespace bustub
