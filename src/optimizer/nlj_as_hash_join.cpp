#include <algorithm>
#include <memory>
#include <utility>
#include <vector>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
      if (expr->comp_type_ == ComparisonType::Equal) {
        if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
            left_expr != nullptr) {
          if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
              right_expr != nullptr) {
            auto left_expr_tuple_0 =
                std::make_shared<ColumnValueExpression>(0, left_expr->GetColIdx(), left_expr->GetReturnType());
            auto right_expr_tuple_0 =
                std::make_shared<ColumnValueExpression>(0, right_expr->GetColIdx(), right_expr->GetReturnType());
            std::vector<AbstractExpressionRef> left_exprs{left_expr_tuple_0};
            std::vector<AbstractExpressionRef> right_exprs{right_expr_tuple_0};
            if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
              return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                        nlj_plan.GetRightPlan(), std::move(left_exprs),
                                                        std::move(right_exprs), nlj_plan.GetJoinType());
            }
            if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
              return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                        nlj_plan.GetRightPlan(), std::move(right_exprs),
                                                        std::move(left_exprs), nlj_plan.GetJoinType());
            }
          }
        }
      }
    }
    if (const auto *expr = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
      if (expr->logic_type_ == LogicType::And) {
        if (const auto *lexpr = dynamic_cast<const ComparisonExpression *>(expr->children_[0].get());
            lexpr != nullptr) {
          if (const auto *llexpr = dynamic_cast<const ColumnValueExpression *>(lexpr->children_[0].get());
              llexpr != nullptr) {
            if (const auto *lrexpr = dynamic_cast<const ColumnValueExpression *>(lexpr->children_[1].get());
                lrexpr != nullptr) {
              if (const auto *rexpr = dynamic_cast<const ComparisonExpression *>(expr->children_[1].get());
                  rexpr != nullptr) {
                if (const auto *rlexpr = dynamic_cast<const ColumnValueExpression *>(rexpr->children_[0].get());
                    rlexpr != nullptr) {
                  if (const auto *rrexpr = dynamic_cast<const ColumnValueExpression *>(rexpr->children_[1].get());
                      rrexpr != nullptr) {
                    if (llexpr->GetTupleIdx() == rlexpr->GetTupleIdx() &&
                        lrexpr->GetTupleIdx() == rrexpr->GetTupleIdx()) {
                      auto llexpr_tuple_0 =
                          std::make_shared<ColumnValueExpression>(0, llexpr->GetColIdx(), llexpr->GetReturnType());
                      auto lrexpr_tuple_0 =
                          std::make_shared<ColumnValueExpression>(0, lrexpr->GetColIdx(), lrexpr->GetReturnType());
                      auto rlexpr_tuple_0 =
                          std::make_shared<ColumnValueExpression>(0, rlexpr->GetColIdx(), rlexpr->GetReturnType());
                      auto rrexpr_tuple_0 =
                          std::make_shared<ColumnValueExpression>(0, rrexpr->GetColIdx(), rrexpr->GetReturnType());
                      std::vector<AbstractExpressionRef> left_exprs{llexpr_tuple_0, rlexpr_tuple_0};
                      std::vector<AbstractExpressionRef> right_exprs{lrexpr_tuple_0, rrexpr_tuple_0};
                      if (llexpr->GetTupleIdx() == 0 && rrexpr->GetTupleIdx() == 1) {
                        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                                  nlj_plan.GetRightPlan(), std::move(left_exprs),
                                                                  std::move(right_exprs), nlj_plan.GetJoinType());
                      }
                      if (llexpr->GetTupleIdx() == 1 && rrexpr->GetTupleIdx() == 0) {
                        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                                  nlj_plan.GetRightPlan(), std::move(right_exprs),
                                                                  std::move(left_exprs), nlj_plan.GetJoinType());
                      }
                    }
                    if (llexpr->GetTupleIdx() == rrexpr->GetTupleIdx() &&
                        lrexpr->GetTupleIdx() == rlexpr->GetTupleIdx()) {
                      auto llexpr_tuple_0 =
                          std::make_shared<ColumnValueExpression>(0, llexpr->GetColIdx(), llexpr->GetReturnType());
                      auto lrexpr_tuple_0 =
                          std::make_shared<ColumnValueExpression>(0, lrexpr->GetColIdx(), lrexpr->GetReturnType());
                      auto rlexpr_tuple_0 =
                          std::make_shared<ColumnValueExpression>(0, rlexpr->GetColIdx(), rlexpr->GetReturnType());
                      auto rrexpr_tuple_0 =
                          std::make_shared<ColumnValueExpression>(0, rrexpr->GetColIdx(), rrexpr->GetReturnType());
                      std::vector<AbstractExpressionRef> left_exprs{llexpr_tuple_0, rrexpr_tuple_0};
                      std::vector<AbstractExpressionRef> right_exprs{lrexpr_tuple_0, rlexpr_tuple_0};
                      if (llexpr->GetTupleIdx() == 0 && lrexpr->GetTupleIdx() == 1) {
                        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                                  nlj_plan.GetRightPlan(), std::move(left_exprs),
                                                                  std::move(right_exprs), nlj_plan.GetJoinType());
                      }
                      if (llexpr->GetTupleIdx() == 1 && lrexpr->GetTupleIdx() == 0) {
                        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                                  nlj_plan.GetRightPlan(), std::move(right_exprs),
                                                                  std::move(left_exprs), nlj_plan.GetJoinType());
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
