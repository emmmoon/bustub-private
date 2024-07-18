#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <ostream>
#include <unordered_map>
#include <utility>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "binder/tokens.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/arithmetic_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/mock_scan_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "execution/plans/values_plan.h"
#include "optimizer/optimizer.h"
#include "type/boolean_type.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value_factory.h"

namespace bustub {

void OptimizerHelperFunction() {}

auto Optimizer::OptimizeConstantFolder(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeConstantFolder(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nestedloopjoin_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    const auto &expr = nestedloopjoin_plan.predicate_;
    return std::make_shared<NestedLoopJoinPlanNode>(
        nestedloopjoin_plan.output_schema_, nestedloopjoin_plan.GetLeftPlan(), nestedloopjoin_plan.GetRightPlan(),
        ConstantFolderExpr(expr), nestedloopjoin_plan.join_type_);
  }

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    const auto &expr = filter_plan.predicate_;
    return std::make_shared<FilterPlanNode>(filter_plan.output_schema_, ConstantFolderExpr(expr),
                                            filter_plan.GetChildPlan());
  }

  return optimized_plan;
}

auto Optimizer::ConstantFolderExpr(const AbstractExpressionRef &expr) -> AbstractExpressionRef {
  std::vector<AbstractExpressionRef> children;
  for (const auto &child : expr->GetChildren()) {
    children.emplace_back(ConstantFolderExpr(child));
  }
  auto optimized_expr = expr->CloneWithChildren(std::move(children));

  if (const auto *arithemetic_expr = dynamic_cast<const ArithmeticExpression *>(optimized_expr.get());
      arithemetic_expr != nullptr) {
    if (const auto *left_constant_expr =
            dynamic_cast<const ConstantValueExpression *>(arithemetic_expr->children_[0].get())) {
      if (const auto *right_constant_expr =
              dynamic_cast<const ConstantValueExpression *>(arithemetic_expr->children_[1].get())) {
        if (arithemetic_expr->compute_type_ == ArithmeticType::Plus) {
          auto value = left_constant_expr->val_.Add(right_constant_expr->val_);
          return std::make_shared<ConstantValueExpression>(value);
        }
        auto value = left_constant_expr->val_.Subtract(right_constant_expr->val_);
        return std::make_shared<ConstantValueExpression>(value);
      }
    }
  }

  if (const auto *comparison_expr = dynamic_cast<const ComparisonExpression *>(optimized_expr.get());
      comparison_expr != nullptr) {
    if (const auto *left_constant_expr =
            dynamic_cast<const ConstantValueExpression *>(comparison_expr->children_[0].get())) {
      if (const auto *right_constant_expr =
              dynamic_cast<const ConstantValueExpression *>(comparison_expr->children_[1].get())) {
        switch (comparison_expr->comp_type_) {
          case ComparisonType::Equal:
            // std::cout << "=" << std::endl;
            return std::make_shared<ConstantValueExpression>(
                ValueFactory::GetBooleanValue(left_constant_expr->val_.CompareEquals(right_constant_expr->val_)));
          case ComparisonType::NotEqual:
            // std::cout << "!=" << std::endl;
            return std::make_shared<ConstantValueExpression>(
                ValueFactory::GetBooleanValue(left_constant_expr->val_.CompareNotEquals(right_constant_expr->val_)));
          case ComparisonType::GreaterThanOrEqual:
            // std::cout << ">=" << std::endl;
            return std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(
                left_constant_expr->val_.CompareGreaterThanEquals(right_constant_expr->val_)));
          case ComparisonType::GreaterThan:
            // std::cout << ">" << std::endl;
            return std::make_shared<ConstantValueExpression>(
                ValueFactory::GetBooleanValue(left_constant_expr->val_.CompareGreaterThan(right_constant_expr->val_)));
          case ComparisonType::LessThan:
            // std::cout << "<" << std::endl;
            return std::make_shared<ConstantValueExpression>(
                ValueFactory::GetBooleanValue(left_constant_expr->val_.CompareLessThan(right_constant_expr->val_)));
          case ComparisonType::LessThanOrEqual:
            // std::cout << "<=" << std::endl;
            return std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(
                left_constant_expr->val_.CompareLessThanEquals(right_constant_expr->val_)));
        }
      }
    }
  }

  if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(optimized_expr.get()); logic_expr != nullptr) {
    if (logic_expr->logic_type_ == LogicType::And) {
      if (const auto *left_constant_expr =
              dynamic_cast<const ConstantValueExpression *>(logic_expr->children_[0].get())) {
        if (!left_constant_expr->val_.GetAs<bool>()) {
          // std::cout << "false and x" << std::endl;
          return std::make_shared<ConstantValueExpression>(left_constant_expr->val_);
        }
        if (const auto *right_constant_expr =
                dynamic_cast<const ConstantValueExpression *>(logic_expr->children_[1].get())) {
          // std::cout << "depends on right" << std::endl;
          return std::make_shared<ConstantValueExpression>(right_constant_expr->val_);
        }
      }
      if (const auto *right_constant_expr =
              dynamic_cast<const ConstantValueExpression *>(logic_expr->children_[1].get())) {
        if (!right_constant_expr->val_.GetAs<bool>()) {
          // std::cout << "x and false" << std::endl;
          return std::make_shared<ConstantValueExpression>(right_constant_expr->val_);
        }
      }
    } else {
      if (const auto *left_constant_expr =
              dynamic_cast<const ConstantValueExpression *>(logic_expr->children_[0].get())) {
        if (left_constant_expr->val_.GetAs<bool>()) {
          // std::cout << "true or x" << std::endl;
          return std::make_shared<ConstantValueExpression>(left_constant_expr->val_);
        }
        if (const auto *right_constant_expr =
                dynamic_cast<const ConstantValueExpression *>(logic_expr->children_[1].get())) {
          // std::cout << "depends on right" << std::endl;
          return std::make_shared<ConstantValueExpression>(right_constant_expr->val_);
        }
      }
      if (const auto *right_constant_expr =
              dynamic_cast<const ConstantValueExpression *>(logic_expr->children_[1].get())) {
        if (right_constant_expr->val_.GetAs<bool>()) {
          // std::cout << "x or true" << std::endl;
          return std::make_shared<ConstantValueExpression>(right_constant_expr->val_);
        }
      }
    }
  }

  return optimized_expr;
}

auto Optimizer::OptimizeColumnCut(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  if (plan->GetType() == PlanType::Projection) {
    const auto &projection_plan = dynamic_cast<const ProjectionPlanNode &>(*plan);
    ProjectionPlanNode now_plan = projection_plan;
    if (now_plan.children_[0]->GetType() == PlanType::Projection) {
      std::vector<AbstractExpressionRef> exprs(now_plan.expressions_);
      std::vector<uint32_t> idxs{};
      while (now_plan.children_[0]->GetType() == PlanType::Projection) {
        auto sub_projection_plan = dynamic_cast<const ProjectionPlanNode &>(*now_plan.children_[0]);
        ColumnExprReplace(exprs, sub_projection_plan.expressions_, idxs);
        if (idxs.empty()) {
          break;
        }
        now_plan = sub_projection_plan;
      }
      if (idxs.empty() && now_plan.children_[0]->GetType() == PlanType::Projection) {
        return std::make_shared<ProjectionPlanNode>(projection_plan.output_schema_, exprs, nullptr);
      }
      if (now_plan.children_[0]->GetType() == PlanType::Aggregation) {
        const auto &aggregation_plan = dynamic_cast<const AggregationPlanNode &>(*now_plan.children_[0]);
        auto pre_num = aggregation_plan.group_bys_.size();
        std::sort(idxs.begin(), idxs.end());
        auto end_unique = std::unique(idxs.begin(), idxs.end());
        idxs.erase(end_unique, idxs.end());
        std::unordered_map<AggregationUnit, uint32_t> m;
        std::unordered_map<uint32_t, uint32_t> m_shorten;
        uint32_t new_idx = 0;
        std::vector<AbstractExpressionRef> new_aggregates;
        std::vector<AggregationType> new_aggretypes;
        std::vector<Column> new_columns;
        for (auto &idx : idxs) {
          if (idx < pre_num) {
            new_columns.emplace_back(aggregation_plan.output_schema_->GetColumns()[idx]);
            continue;
          }
          if (const auto *columnvalue_expr =
                  dynamic_cast<const ColumnValueExpression *>(aggregation_plan.aggregates_[idx - pre_num].get())) {
            AggregationUnit u{aggregation_plan.agg_types_[idx - pre_num], columnvalue_expr->GetColIdx()};
            if (m.find(u) == m.end()) {
              m[u] = idx;
              m_shorten[idx] = new_idx + pre_num;
              new_aggregates.emplace_back(std::make_shared<ColumnValueExpression>(
                  columnvalue_expr->GetTupleIdx(), columnvalue_expr->GetColIdx(), columnvalue_expr->GetReturnType()));
              new_aggretypes.emplace_back(u.type_);
              new_columns.emplace_back(aggregation_plan.output_schema_->GetColumns()[idx]);
              ++new_idx;
            } else {
              m_shorten[idx] = m_shorten[m[u]];
            }
          } else {
            new_aggregates.emplace_back(aggregation_plan.aggregates_[idx - pre_num]);
            new_aggretypes.emplace_back(aggregation_plan.agg_types_[idx - pre_num]);
            new_columns.emplace_back(aggregation_plan.output_schema_->GetColumns()[idx]);
            ++new_idx;
          }
        }
        auto new_aggregation_plan = std::make_shared<AggregationPlanNode>(
            std::make_shared<const Schema>(new_columns), aggregation_plan.GetChildPlan(), aggregation_plan.group_bys_,
            new_aggregates, new_aggretypes);
        ColumnExprIdxReplace(exprs, m_shorten, pre_num);
        return std::make_shared<ProjectionPlanNode>(projection_plan.output_schema_, exprs, new_aggregation_plan);
      }
    }
  }
  return plan;
}

auto Optimizer::ColumnExprReplace(std::vector<AbstractExpressionRef> &exprs,
                                  const std::vector<AbstractExpressionRef> &sub_exprs, std::vector<uint32_t> &idxs)
    -> void {
  auto exprs_size = exprs.size();
  idxs.clear();
  for (size_t idx = 0; idx < exprs_size; ++idx) {
    exprs[idx] = ExprReplace(exprs[idx], sub_exprs, idxs);
  }
}

auto Optimizer::ExprReplace(const AbstractExpressionRef &expr, const std::vector<AbstractExpressionRef> &sub_exprs,
                            std::vector<uint32_t> &idxs) -> AbstractExpressionRef {
  std::vector<AbstractExpressionRef> children;
  for (const auto &child : expr->GetChildren()) {
    children.emplace_back(ExprReplace(child, sub_exprs, idxs));
  }
  auto optimized_expr = expr->CloneWithChildren(std::move(children));
  if (const auto *column_expr = dynamic_cast<const ColumnValueExpression *>(optimized_expr.get());
      column_expr != nullptr) {
    auto column_idx = column_expr->GetColIdx();
    IdxsSupplyment(sub_exprs[column_idx], idxs);
    return sub_exprs[column_idx];
  }
  return optimized_expr;
}

auto Optimizer::IdxsSupplyment(const AbstractExpressionRef &expr, std::vector<uint32_t> &idxs) -> void {
  std::vector<AbstractExpressionRef> children;
  for (const auto &child : expr->GetChildren()) {
    IdxsSupplyment(child, idxs);
  }
  if (const auto *column_expr = dynamic_cast<const ColumnValueExpression *>(expr.get())) {
    idxs.emplace_back(column_expr->GetColIdx());
  }
}

auto Optimizer::ColumnExprIdxReplace(std::vector<AbstractExpressionRef> &exprs,
                                     const std::unordered_map<uint32_t, uint32_t> &m_shorten, size_t pre_num) -> void {
  auto exprs_size = exprs.size();
  for (size_t idx = 0; idx < exprs_size; ++idx) {
    exprs[idx] = IdxReplace(exprs[idx], m_shorten, pre_num);
  }
}

auto Optimizer::IdxReplace(const AbstractExpressionRef &expr, const std::unordered_map<uint32_t, uint32_t> &m_shorten,
                           size_t pre_num) -> AbstractExpressionRef {
  std::vector<AbstractExpressionRef> children;
  for (const auto &child : expr->GetChildren()) {
    children.emplace_back(IdxReplace(child, m_shorten, pre_num));
  }
  auto optimized_expr = expr->CloneWithChildren(std::move(children));

  if (const auto *column_expr = dynamic_cast<const ColumnValueExpression *>(optimized_expr.get());
      column_expr != nullptr) {
    auto column_idx = column_expr->GetColIdx();
    auto new_column_idx = column_idx < pre_num ? column_idx : m_shorten.find(column_idx)->second;
    return std::make_shared<ColumnValueExpression>(column_expr->GetTupleIdx(), new_column_idx,
                                                   column_expr->GetReturnType());
  }
  return optimized_expr;
}

auto Optimizer::EstimatedScale(const AbstractPlanNodeRef &plan) -> std::optional<size_t> {
  if (plan->GetType() == PlanType::MockScan) {
    const auto &mockscan_plan = dynamic_cast<const MockScanPlanNode &>(*plan);
    return EstimatedCardinality(mockscan_plan.GetTable());
  }

  if (plan->GetType() == PlanType::SeqScan) {
    const auto &seqscan_plan = dynamic_cast<const SeqScanPlanNode &>(*plan);
    return EstimatedCardinality(seqscan_plan.table_name_);
  }

  if (plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*plan);
    auto left_scale = EstimatedScale(nlj_plan.GetLeftPlan());
    auto right_scale = EstimatedScale(nlj_plan.GetRightPlan());
    if (left_scale.has_value() && right_scale.has_value()) {
      return left_scale.value() + right_scale.value();
    }
    return std::nullopt;
  }

  if (plan->GetType() == PlanType::HashJoin) {
    const auto &hj_plan = dynamic_cast<const HashJoinPlanNode &>(*plan);
    auto left_scale = EstimatedScale(hj_plan.GetLeftPlan());
    auto right_scale = EstimatedScale(hj_plan.GetRightPlan());
    if (left_scale.has_value() && right_scale.has_value()) {
      return left_scale.value() + right_scale.value();
    }
    return std::nullopt;
  }

  if (plan->GetType() == PlanType::Aggregation) {
    const auto &agg_plan = dynamic_cast<const AggregationPlanNode &>(*plan);
    return EstimatedScale(agg_plan.GetChildPlan());
  }

  if (plan->GetType() == PlanType::Projection) {
    const auto &projection_plan = dynamic_cast<const ProjectionPlanNode &>(*plan);
    return EstimatedScale(projection_plan.GetChildPlan());
  }

  if (plan->GetType() == PlanType::Sort) {
    const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*plan);
    return EstimatedScale(sort_plan.GetChildPlan());
  }

  if (plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*plan);
    return EstimatedScale(filter_plan.GetChildPlan());
  }

  if (plan->GetType() == PlanType::TopN) {
    const auto &topn_plan = dynamic_cast<const TopNPlanNode &>(*plan);
    return topn_plan.n_;
  }

  if (plan->GetType() == PlanType::Limit) {
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*plan);
    return limit_plan.limit_;
  }

  if (plan->GetType() == PlanType::Values) {
    const auto &values_plan = dynamic_cast<const ValuesPlanNode &>(*plan);
    return values_plan.values_.size();
  }

  return std::nullopt;
}

auto Optimizer::OptimizeJoinOrder(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeJoinOrder(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    auto left_scale = EstimatedScale(nlj_plan.GetLeftPlan());
    auto right_scale = EstimatedScale(nlj_plan.GetRightPlan());
    if (left_scale.has_value() && left_scale.has_value() && left_scale.value() > right_scale.value()) {
      AbstractExpressionRef new_exprs{nlj_plan.predicate_};
      new_exprs = ExchangeTableID(new_exprs);
      return std::make_shared<NestedLoopJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetRightPlan(),
                                                      nlj_plan.GetLeftPlan(), new_exprs, nlj_plan.join_type_);
    }
  }
  return optimized_plan;
}

auto Optimizer::ExchangeTableID(const AbstractExpressionRef &expr) -> AbstractExpressionRef {
  std::vector<AbstractExpressionRef> chilren;
  for (auto &child : expr->children_) {
    chilren.emplace_back(ExchangeTableID(child));
  }
  auto optimized_expr = expr->CloneWithChildren(chilren);

  if (auto *columnvalue_expr = dynamic_cast<ColumnValueExpression *>(expr.get())) {
    auto new_table_id = columnvalue_expr->GetTupleIdx() == 0 ? 1 : 0;
    return std::make_shared<ColumnValueExpression>(new_table_id, columnvalue_expr->GetColIdx(),
                                                   columnvalue_expr->GetReturnType());
  }

  return optimized_expr;
}

auto Optimizer::OptimizePredicatePushDown(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  AbstractPlanNodeRef optimized_plan;
  if (plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nestedloopjoin_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*plan);
    AbstractExpressionRef lt_pushdown_exprs = nullptr;
    AbstractExpressionRef rt_pushdown_exprs = nullptr;
    AbstractPlanNodeRef new_left_child_plan;
    AbstractPlanNodeRef new_right_child_plan;
    AbstractExpressionRef new_expr;
    auto new_expr_opt = GetPushDownExprs(lt_pushdown_exprs, rt_pushdown_exprs, nestedloopjoin_plan.predicate_);
    if (new_expr_opt.has_value()) {
      new_expr = new_expr_opt.value();
    } else {
      new_expr = std::make_shared<ConstantValueExpression>(ValueFactory::GetBooleanValue(true));
    }
    if (lt_pushdown_exprs != nullptr && nestedloopjoin_plan.GetLeftPlan()->GetType() == PlanType::NestedLoopJoin) {
      const auto &left_nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*nestedloopjoin_plan.GetLeftPlan());
      lt_pushdown_exprs = DevideTheSameSideNLJPredicate(lt_pushdown_exprs,
                                                        left_nlj_plan.GetLeftPlan()->output_schema_->GetColumnCount());
      AbstractExpressionRef new_predicate;
      if (const auto *constantexpr = dynamic_cast<const ConstantValueExpression *>(left_nlj_plan.predicate_.get());
          constantexpr->val_.GetAs<bool>()) {
        new_predicate = lt_pushdown_exprs;
      } else {
        new_predicate = std::make_shared<LogicExpression>(left_nlj_plan.predicate_, lt_pushdown_exprs, LogicType::And);
      }

      new_left_child_plan = std::make_shared<NestedLoopJoinPlanNode>(
          left_nlj_plan.output_schema_, left_nlj_plan.children_[0], left_nlj_plan.children_[1], new_predicate,
          left_nlj_plan.GetJoinType());
    } else if (lt_pushdown_exprs != nullptr) {
      new_left_child_plan = std::make_shared<FilterPlanNode>(nestedloopjoin_plan.GetLeftPlan()->output_schema_,
                                                             lt_pushdown_exprs, nestedloopjoin_plan.children_[0]);
    } else {
      new_left_child_plan = nestedloopjoin_plan.GetLeftPlan();
    }

    if (rt_pushdown_exprs != nullptr && nestedloopjoin_plan.GetRightPlan()->GetType() == PlanType::NestedLoopJoin) {
      const auto &right_nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*nestedloopjoin_plan.GetRightPlan());
      rt_pushdown_exprs = DevideTheSameSideNLJPredicate(rt_pushdown_exprs,
                                                        right_nlj_plan.GetLeftPlan()->output_schema_->GetColumnCount());
      AbstractExpressionRef new_predicate;
      if (const auto *constantexpr = dynamic_cast<const ConstantValueExpression *>(right_nlj_plan.predicate_.get());
          constantexpr->val_.GetAs<bool>()) {
        new_predicate = rt_pushdown_exprs;
      } else {
        new_predicate = std::make_shared<LogicExpression>(right_nlj_plan.predicate_, rt_pushdown_exprs, LogicType::And);
      }

      new_right_child_plan = std::make_shared<NestedLoopJoinPlanNode>(
          right_nlj_plan.output_schema_, right_nlj_plan.children_[0], right_nlj_plan.children_[1], new_predicate,
          right_nlj_plan.GetJoinType());
    } else if (rt_pushdown_exprs != nullptr) {
      new_right_child_plan = std::make_shared<FilterPlanNode>(nestedloopjoin_plan.GetRightPlan()->output_schema_,
                                                              rt_pushdown_exprs, nestedloopjoin_plan.children_[1]);
    } else {
      new_right_child_plan = nestedloopjoin_plan.GetRightPlan();
    }
    optimized_plan =
        std::make_shared<NestedLoopJoinPlanNode>(nestedloopjoin_plan.output_schema_, new_left_child_plan,
                                                 new_right_child_plan, new_expr, nestedloopjoin_plan.join_type_);
  } else {
    optimized_plan = plan;
  }
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : optimized_plan->GetChildren()) {
    children.emplace_back(OptimizePredicatePushDown(child));
  }
  return optimized_plan->CloneWithChildren(children);
}

auto Optimizer::GetPushDownExprs(AbstractExpressionRef &lt_pushdown_exprs, AbstractExpressionRef &rt_pushdown_exprs,
                                 const AbstractExpressionRef &expr) -> std::optional<AbstractExpressionRef> {
  std::vector<AbstractExpressionRef> children;
  for (const auto &child : expr->GetChildren()) {
    auto new_child = GetPushDownExprs(lt_pushdown_exprs, rt_pushdown_exprs, child);
    if (new_child.has_value()) {
      children.emplace_back(new_child.value());
    }
  }

  auto optimized_expr = expr->CloneWithChildren(std::move(children));

  if (auto *logic_expr = dynamic_cast<LogicExpression *>(expr.get())) {
    if (optimized_expr->GetChildren().empty()) {
      return std::nullopt;
    }
    if (optimized_expr->GetChildren().size() == 1) {
      return optimized_expr->children_[0];
    }
    bool left_pushed = false;
    bool right_pushed = false;
    auto *real_logic_expr = dynamic_cast<LogicExpression *>(optimized_expr.get());
    if (auto *left_comparison_expr = dynamic_cast<ComparisonExpression *>(real_logic_expr->children_[0].get())) {
      if (auto *left_expr = dynamic_cast<ColumnValueExpression *>(left_comparison_expr->children_[0].get())) {
        if (auto *right_expr = dynamic_cast<ConstantValueExpression *>(left_comparison_expr->children_[1].get())) {
          left_pushed = true;
          if (left_expr->GetTupleIdx() == 0) {
            if (lt_pushdown_exprs == nullptr) {
              lt_pushdown_exprs = real_logic_expr->children_[0];
            } else {
              lt_pushdown_exprs = std::make_shared<LogicExpression>(lt_pushdown_exprs, real_logic_expr->children_[0],
                                                                    real_logic_expr->logic_type_);
            }
          } else {
            if (rt_pushdown_exprs == nullptr) {
              rt_pushdown_exprs = real_logic_expr->children_[0];
              rt_pushdown_exprs = ExchangeTableID(rt_pushdown_exprs);
            } else {
              AbstractExpressionRef new_pushed_right_expr = real_logic_expr->children_[0];
              new_pushed_right_expr = ExchangeTableID(new_pushed_right_expr);
              rt_pushdown_exprs = std::make_shared<LogicExpression>(rt_pushdown_exprs, new_pushed_right_expr,
                                                                    real_logic_expr->logic_type_);
            }
          }
        }
        if (auto *right_expr = dynamic_cast<ColumnValueExpression *>(left_comparison_expr->children_[1].get())) {
          if (left_expr->GetTupleIdx() == right_expr->GetTupleIdx()) {
            left_pushed = true;
            if (left_expr->GetTupleIdx() == 0) {
              if (lt_pushdown_exprs == nullptr) {
                lt_pushdown_exprs = real_logic_expr->children_[0];
              } else {
                lt_pushdown_exprs = std::make_shared<LogicExpression>(lt_pushdown_exprs, real_logic_expr->children_[0],
                                                                      real_logic_expr->logic_type_);
              }
            } else {
              if (rt_pushdown_exprs == nullptr) {
                rt_pushdown_exprs = real_logic_expr->children_[0];
                rt_pushdown_exprs = ExchangeTableID(rt_pushdown_exprs);
              } else {
                AbstractExpressionRef new_pushed_right_expr = real_logic_expr->children_[0];
                new_pushed_right_expr = ExchangeTableID(new_pushed_right_expr);
                rt_pushdown_exprs = std::make_shared<LogicExpression>(rt_pushdown_exprs, new_pushed_right_expr,
                                                                      real_logic_expr->logic_type_);
              }
            }
          }
        }
      }
    }
    if (auto *right_comparison_expr = dynamic_cast<ComparisonExpression *>(real_logic_expr->children_[1].get())) {
      if (auto *left_expr = dynamic_cast<ColumnValueExpression *>(right_comparison_expr->children_[0].get())) {
        if (auto *right_expr = dynamic_cast<ConstantValueExpression *>(right_comparison_expr->children_[1].get())) {
          right_pushed = true;
          if (left_expr->GetTupleIdx() == 0) {
            if (lt_pushdown_exprs == nullptr) {
              lt_pushdown_exprs = real_logic_expr->children_[1];
            } else {
              lt_pushdown_exprs = std::make_shared<LogicExpression>(lt_pushdown_exprs, real_logic_expr->children_[1],
                                                                    real_logic_expr->logic_type_);
            }
          } else {
            if (rt_pushdown_exprs == nullptr) {
              rt_pushdown_exprs = real_logic_expr->children_[1];
              rt_pushdown_exprs = ExchangeTableID(rt_pushdown_exprs);
            } else {
              AbstractExpressionRef new_pushed_right_expr = real_logic_expr->children_[1];
              new_pushed_right_expr = ExchangeTableID(new_pushed_right_expr);
              rt_pushdown_exprs = std::make_shared<LogicExpression>(rt_pushdown_exprs, new_pushed_right_expr,
                                                                    real_logic_expr->logic_type_);
            }
          }
        }
        if (auto *right_expr = dynamic_cast<ColumnValueExpression *>(right_comparison_expr->children_[1].get())) {
          if (left_expr->GetTupleIdx() == right_expr->GetTupleIdx()) {
            right_pushed = true;
            if (left_expr->GetTupleIdx() == 0) {
              if (lt_pushdown_exprs == nullptr) {
                lt_pushdown_exprs = real_logic_expr->children_[1];
              } else {
                lt_pushdown_exprs = std::make_shared<LogicExpression>(lt_pushdown_exprs, real_logic_expr->children_[1],
                                                                      real_logic_expr->logic_type_);
              }
            } else {
              if (rt_pushdown_exprs == nullptr) {
                rt_pushdown_exprs = real_logic_expr->children_[1];
                rt_pushdown_exprs = ExchangeTableID(rt_pushdown_exprs);
              } else {
                AbstractExpressionRef new_pushed_right_expr = real_logic_expr->children_[1];
                new_pushed_right_expr = ExchangeTableID(new_pushed_right_expr);
                rt_pushdown_exprs = std::make_shared<LogicExpression>(rt_pushdown_exprs, new_pushed_right_expr,
                                                                      real_logic_expr->logic_type_);
              }
            }
          }
        }
      }
    }
    if (left_pushed && right_pushed) {
      return std::nullopt;
    }
    if (left_pushed) {
      return real_logic_expr->children_[1];
    }
    if (right_pushed) {
      return real_logic_expr->children_[0];
    }
  }
  return optimized_expr;
}

auto Optimizer::DevideTheSameSideNLJPredicate(const AbstractExpressionRef &expr, uint32_t left_num)
    -> AbstractExpressionRef {
  std::vector<AbstractExpressionRef> chilren;
  for (auto &child : expr->children_) {
    chilren.emplace_back(DevideTheSameSideNLJPredicate(child, left_num));
  }
  auto optimized_expr = expr->CloneWithChildren(chilren);
  if (auto *columnvalue_expr = dynamic_cast<ColumnValueExpression *>(expr.get())) {
    if (columnvalue_expr->GetColIdx() >= left_num) {
      // auto new_table_id = columnvalue_expr->GetTupleIdx() == 0 ? 1:0;
      return std::make_shared<ColumnValueExpression>(1, columnvalue_expr->GetColIdx() - left_num,
                                                     columnvalue_expr->GetReturnType());
    }
  }
  return optimized_expr;
}

auto Optimizer::OptimizeAbortUselessFilter(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeJoinOrder(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    if (const auto *constant_value_expression =
            dynamic_cast<const ConstantValueExpression *>(filter_plan.predicate_.get())) {
      if (constant_value_expression->val_.GetAs<bool>()) {
        return optimized_plan->children_[0];
      }
      std::vector<Column> void_columns{};
      auto void_schema_ref = std::make_shared<const Schema>(void_columns);
      std::vector<std::vector<AbstractExpressionRef>> void_exprs;
      return std::make_shared<ValuesPlanNode>(void_schema_ref, void_exprs);
    }
  }
  return optimized_plan;
}

auto Optimizer::OptimizeSeqToIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqToIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    if (seq_plan.filter_predicate_ != nullptr) {
      if (CanConvertToIndexScan(seq_plan.filter_predicate_)) {
        std::vector<uint32_t> col_idxs{};
        std::vector<Value> range_begin{};
        std::vector<Value> range_end{};
        auto new_expr = PickOutRangeFromExpr(seq_plan.filter_predicate_, col_idxs, range_begin, range_end);
        if (col_idxs.empty() || col_idxs.size() > 2) {
          throw NotImplementedException("only support creating index with exactly one or two columns");
        }
        for (size_t idx = 0; idx < col_idxs.size(); ++idx) {
          if (range_begin[idx].GetAs<int>() > range_end[idx].GetAs<int>() - 1) {
            std::vector<Column> void_columns{};
            auto void_schema_ref = std::make_shared<const Schema>(void_columns);
            std::vector<std::vector<AbstractExpressionRef>> void_exprs;
            return std::make_shared<ValuesPlanNode>(void_schema_ref, void_exprs);
          }
        }
        auto indexs = catalog_.GetTableIndexes(seq_plan.table_name_);
        auto table = catalog_.GetTable(seq_plan.table_name_);
        auto key_schema = Schema::CopySchema(&table->schema_, col_idxs);
        std::optional<index_oid_t> index_oid = std::nullopt;
        for (auto index : indexs) {
          if (index->key_schema_.GetColumnCount() == key_schema.GetColumnCount()) {
            if (key_schema.GetColumnCount() == 1) {
              if (key_schema.GetColumns()[0].GetName() == index->key_schema_.GetColumns()[0].GetName()) {
                index_oid = index->index_oid_;
              }
            } else {
              if ((key_schema.GetColumns()[0].GetName() == index->key_schema_.GetColumns()[0].GetName()) &&
                  (key_schema.GetColumns()[1].GetName() == index->key_schema_.GetColumns()[1].GetName())) {
                index_oid = index->index_oid_;
              } else if ((key_schema.GetColumns()[0].GetName() == index->key_schema_.GetColumns()[1].GetName()) &&
                         (key_schema.GetColumns()[1].GetName() == index->key_schema_.GetColumns()[0].GetName())) {
                index_oid = index->index_oid_;
                std::swap(range_begin[0], range_begin[1]);
                std::swap(range_end[0], range_end[1]);
                std::swap(col_idxs[0], col_idxs[1]);
                key_schema = Schema::CopySchema(&table->schema_, col_idxs);
              }
            }
          }
          if (index_oid.has_value()) {
            break;
          }
        }
        if (index_oid.has_value()) {
          return std::make_shared<IndexScanPlanNode>(seq_plan.output_schema_, index_oid.value(),
                                                     std::make_shared<Schema>(key_schema), range_begin, range_end,
                                                     new_expr);
        }
      }
    }
  }
  return optimized_plan;
}

auto Optimizer::PickOutRangeFromExpr(const AbstractExpressionRef &expr, std::vector<uint32_t> &col_idxs,
                                     std::vector<Value> &range_begin, std::vector<Value> &range_end)
    -> AbstractExpressionRef {
  std::vector<AbstractExpressionRef> chilren;
  for (auto &child : expr->children_) {
    chilren.emplace_back(PickOutRangeFromExpr(child, col_idxs, range_begin, range_end));
  }
  auto optimized_expr = expr->CloneWithChildren(chilren);

  if (auto *comparison_expr = dynamic_cast<ComparisonExpression *>(optimized_expr.get())) {
    if (auto *column_expr = dynamic_cast<ColumnValueExpression *>(comparison_expr->children_[0].get())) {
      if (auto *constant_expr = dynamic_cast<ConstantValueExpression *>(comparison_expr->children_[1].get());
          constant_expr->val_.CheckInteger()) {
        auto colidx = column_expr->GetColIdx();
        size_t idx = 2;
        for (size_t i = 0; i < col_idxs.size(); ++i) {
          if (col_idxs[i] == colidx) {
            idx = i;
          }
        }
        if (idx == 2 && col_idxs.size() == 2) {
          return optimized_expr;
        }
        if (idx == 2 && col_idxs.size() < 2) {
          col_idxs.emplace_back(colidx);
          switch (comparison_expr->comp_type_) {
            case ComparisonType::Equal: {
              range_begin.emplace_back(constant_expr->val_);
              range_end.emplace_back(constant_expr->val_.Add(ValueFactory::GetIntegerValue(1)));
              break;
            }
            case ComparisonType::GreaterThan: {
              range_begin.emplace_back(constant_expr->val_.Add(ValueFactory::GetIntegerValue(1)));
              range_end.emplace_back(ValueFactory::GetIntegerValue(BUSTUB_INT32_MAX));
              break;
            }
            case ComparisonType::GreaterThanOrEqual: {
              range_begin.emplace_back(constant_expr->val_);
              range_end.emplace_back(ValueFactory::GetIntegerValue(BUSTUB_INT32_MAX));
              break;
            }
            case ComparisonType::LessThan: {
              range_begin.emplace_back(ValueFactory::GetIntegerValue(BUSTUB_INT32_MIN));
              range_end.emplace_back(constant_expr->val_);
              break;
            }
            case ComparisonType::LessThanOrEqual: {
              range_begin.emplace_back(ValueFactory::GetIntegerValue(BUSTUB_INT32_MIN));
              range_end.emplace_back(constant_expr->val_.Add(ValueFactory::GetIntegerValue(1)));
              break;
            }
            default:
              return optimized_expr;
          }
          return optimized_expr;
        }
        if (idx < 2) {
          switch (comparison_expr->comp_type_) {
            case ComparisonType::Equal: {
              auto equal_num = constant_expr->val_.GetAs<int>();
              if (equal_num > range_end[idx].GetAs<int>() - 1) {
                range_begin[idx] = ValueFactory::GetIntegerValue(equal_num);
              } else if (equal_num < range_begin[idx].GetAs<int>()) {
                range_end[idx] = ValueFactory::GetIntegerValue(equal_num + 1);
              } else {
                range_begin[idx] = ValueFactory::GetIntegerValue(equal_num);
                range_end[idx] = ValueFactory::GetIntegerValue(equal_num + 1);
              }
              break;
            }
            case ComparisonType::GreaterThan: {
              range_begin[idx] = constant_expr->val_.Add(ValueFactory::GetIntegerValue(1));
              break;
            }
            case ComparisonType::GreaterThanOrEqual: {
              range_begin[idx] = constant_expr->val_;
              break;
            }
            case ComparisonType::LessThan: {
              range_end[idx] = constant_expr->val_;
              break;
            }
            case ComparisonType::LessThanOrEqual: {
              range_end[idx] = constant_expr->val_.Add(ValueFactory::GetIntegerValue(1));
              break;
            }
            default:
              return optimized_expr;
          }
          return optimized_expr;
        }
      }
    }
  }
  return optimized_expr;
}

auto Optimizer::CanConvertToIndexScan(const AbstractExpressionRef &expr) -> bool {
  if (auto *comparison_expr = dynamic_cast<ComparisonExpression *>(expr.get())) {
    if (comparison_expr->comp_type_ != ComparisonType::NotEqual) {
      if (auto *left_expr = dynamic_cast<ColumnValueExpression *>(comparison_expr->children_[0].get())) {
        if (auto *right_expr = dynamic_cast<ConstantValueExpression *>(comparison_expr->children_[1].get());
            right_expr->val_.CheckInteger()) {
          return true;
        }
      }
    }
  }

  if (auto *logic_expr = dynamic_cast<LogicExpression *>(expr.get())) {
    if (logic_expr->logic_type_ == LogicType::And) {
      return CanConvertToIndexScan(logic_expr->children_[0]) && CanConvertToIndexScan(logic_expr->children_[1]);
    }
  }
  return false;
}

}  // namespace bustub
