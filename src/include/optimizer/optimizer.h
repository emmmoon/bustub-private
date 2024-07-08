#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/aggregation_plan.h"
#include "type/type.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * The optimizer takes an `AbstractPlanNode` and outputs an optimized `AbstractPlanNode`.
 */
class Optimizer {
 public:
  explicit Optimizer(const Catalog &catalog, bool force_starter_rule)
      : catalog_(catalog), force_starter_rule_(force_starter_rule) {}

  auto Optimize(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

 private:
  /**
   * @brief merge projections that do identical project.
   * Identical projection might be produced when there's `SELECT *`, aggregation, or when we need to rename the columns
   * in the planner. We merge these projections so as to make execution faster.
   */
  auto OptimizeMergeProjection(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief merge filter condition into nested loop join.
   * In planner, we plan cross join + filter with cross product (done with nested loop join) and a filter plan node. We
   * can merge the filter condition into nested loop join to achieve better efficiency.
   */
  auto OptimizeMergeFilterNLJ(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief optimize nested loop join into hash join.
   * In the starter code, we will check NLJs with exactly one equal condition. You can further support optimizing joins
   * with multiple eq conditions.
   */
  auto OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief optimize nested loop join into index join.
   */
  auto OptimizeNLJAsIndexJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief eliminate always true filter
   */
  auto OptimizeEliminateTrueFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief merge filter into filter_predicate of seq scan plan node
   */
  auto OptimizeMergeFilterScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief rewrite expression to be used in nested loop joins. e.g., if we have `SELECT * FROM a, b WHERE a.x = b.y`,
   * we will have `#0.x = #0.y` in the filter plan node. We will need to figure out where does `0.x` and `0.y` belong
   * in NLJ (left table or right table?), and rewrite it as `#0.x = #1.y`.
   *
   * @param expr the filter expression
   * @param left_column_cnt number of columns in the left size of the NLJ
   * @param right_column_cnt number of columns in the left size of the NLJ
   */
  auto RewriteExpressionForJoin(const AbstractExpressionRef &expr, size_t left_column_cnt, size_t right_column_cnt)
      -> AbstractExpressionRef;

  /** @brief check if the predicate is true::boolean */
  auto IsPredicateTrue(const AbstractExpressionRef &expr) -> bool;

  /**
   * @brief optimize order by as index scan if there's an index on a table
   */
  auto OptimizeOrderByAsIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /** @brief check if the index can be matched */
  auto MatchIndex(const std::string &table_name, uint32_t index_key_idx)
      -> std::optional<std::tuple<index_oid_t, std::string>>;

  /**
   * @brief optimize sort + limit as top N
   */
  auto OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  /**
   * @brief get the estimated cardinality for a table based on the table name. Useful when join reordering. BusTub
   * doesn't support statistics for now, so it's the only way for you to get the table size :(
   *
   * @param table_name
   * @return std::optional<size_t>
   */
  auto EstimatedCardinality(const std::string &table_name) -> std::optional<size_t>;

  /** Catalog will be used during the planning process. USERS SHOULD ENSURE IT OUTLIVES
   * OPTIMIZER, otherwise it's a dangling reference.
   */

  auto OptimizeConstantFolder(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto ConstantFolderExpr(const AbstractExpressionRef &expr) -> AbstractExpressionRef;

  auto OptimizeColumnCut(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto ColumnExprReplace(std::vector<AbstractExpressionRef> &exprs, const std::vector<AbstractExpressionRef> &sub_exprs,
                         std::vector<uint32_t> &idxs) -> void;

  auto ExprReplace(const AbstractExpressionRef &expr, const std::vector<AbstractExpressionRef> &sub_exprs,
                   std::vector<uint32_t> &idxs) -> AbstractExpressionRef;

  auto IdxsSupplyment(const AbstractExpressionRef &expr, std::vector<uint32_t> &idxs) -> void;

  auto ColumnExprIdxReplace(std::vector<AbstractExpressionRef> &exprs,
                            const std::unordered_map<uint32_t, uint32_t> &m_shorten, size_t pre_num) -> void;

  auto IdxReplace(const AbstractExpressionRef &expr, const std::unordered_map<uint32_t, uint32_t> &m_shorten,
                  size_t pre_num) -> AbstractExpressionRef;

  auto EstimatedScale(const AbstractPlanNodeRef &plan) -> std::optional<size_t>;

  auto OptimizeJoinOrder(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto ExchangeTableID(const AbstractExpressionRef &expr) -> AbstractExpressionRef;

  auto OptimizePredicatePushDown(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto GetPushDownExprs(AbstractExpressionRef &lt_pushdown_exprs, AbstractExpressionRef &rt_pushdown_exprs,
                        const AbstractExpressionRef &expr) -> std::optional<AbstractExpressionRef>;

  auto DevideTheSameSideNLJPredicate(const AbstractExpressionRef &expr, uint32_t left_num) -> AbstractExpressionRef;

  auto OptimizeAbortUselessFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto OptimizeSeqToIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef;

  auto PickOutRangeFromExpr(const AbstractExpressionRef &expr, std::vector<uint32_t> &col_idxs,
                            std::vector<Value> &range_begin, std::vector<Value> &range_end) -> AbstractExpressionRef;

  auto CanConvertToIndexScan(const AbstractExpressionRef &expr) -> bool;

  const Catalog &catalog_;

  const bool force_starter_rule_;
};

struct AggregationUnit {
  AggregationType type_;
  uint32_t cid_;

  auto operator==(const AggregationUnit &other) const -> bool { return type_ == other.type_ && cid_ == other.cid_; }
};

}  // namespace bustub

namespace std {

/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::AggregationUnit> {
  auto operator()(const bustub::AggregationUnit &agg_unit) const -> std::size_t {
    size_t curr_hash = 0;
    // for (const auto &key : agg_key.group_bys_) {
    //   if (!key.IsNull()) {
    //     curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
    //   }
    // }
    auto agg_type = bustub::ValueFactory::GetIntegerValue(static_cast<uint32_t>(agg_unit.type_));
    auto agg_cid = bustub::ValueFactory::GetIntegerValue(agg_unit.cid_);
    curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&agg_type));
    curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&agg_cid));
    return curr_hash;
  }
};

}  // namespace std
