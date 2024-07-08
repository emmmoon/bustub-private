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
#include <memory>
#include <string>
#include "include/storage/index/b_plus_tree_index.h"
#include "storage/table/tuple.h"

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

void IndexScanExecutor::Init() {
  if (!convert_from_seq_) {
    iter_ = tree_->GetBeginIterator();
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
