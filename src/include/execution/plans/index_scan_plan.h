//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_plan.h
//
// Identification: src/include/execution/plans/index_scan_plan.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"
#include "type/type.h"

namespace bustub {
/**
 * IndexScanPlanNode identifies a table that should be scanned with an optional predicate.
 */
class IndexScanPlanNode : public AbstractPlanNode {
 public:
  /**
   * Creates a new index scan plan node.
   * @param output the output format of this scan plan node
   * @param table_oid the identifier of table to be scanned
   */
  IndexScanPlanNode(SchemaRef output, index_oid_t index_oid, SchemaRef key_schema = nullptr,
                    std::vector<Value> begin = {}, std::vector<Value> end = {},
                    AbstractExpressionRef filter_predicate = nullptr)
      : AbstractPlanNode(std::move(output), {}),
        index_oid_(index_oid),
        filter_predicate_(std::move(filter_predicate)),
        key_schema_(std::move(key_schema)),
        begin_(std::move(begin)),
        end_(std::move(end)) {}

  auto GetType() const -> PlanType override { return PlanType::IndexScan; }

  /** @return the identifier of the table that should be scanned */
  auto GetIndexOid() const -> index_oid_t { return index_oid_; }

  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(IndexScanPlanNode);

  /** The table whose tuples should be scanned. */
  index_oid_t index_oid_;

  AbstractExpressionRef filter_predicate_;

  SchemaRef key_schema_;

  std::vector<Value> begin_;

  std::vector<Value> end_;

  // Add anything you want here for index lookup

 protected:
  auto PlanNodeToString() const -> std::string override {
    return fmt::format("IndexScan {{ index_oid={} }}", index_oid_);
  }
};

}  // namespace bustub
