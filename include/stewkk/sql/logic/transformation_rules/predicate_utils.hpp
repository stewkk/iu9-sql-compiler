#pragma once

#include <string>
#include <unordered_set>
#include <vector>

#include <stewkk/sql/logic/optimizer/group.hpp>
#include <stewkk/sql/models/parser/expression.hpp>
#include <stewkk/sql/utils/not_null.hpp>

namespace stewkk::sql {

bool IsTrue(const Expression& e);

void CollectConjuncts(const Expression& e, std::vector<Expression>& out);

Expression AndConjuncts(const std::vector<Expression>& conjs);

void CollectAttrTables(const Expression& e, std::unordered_set<std::string>& out);

void CollectAttributes(const Expression& e, std::vector<Attribute>& out);

std::unordered_set<std::string> ExprTables(const Expression& e);

std::unordered_set<std::string> GroupTables(utils::NotNull<Group*> g);

bool ExprUsesOnlyTables(const Expression& e, const std::unordered_set<std::string>& tables);

bool IsNullRejectingForTables(const Expression& e, const std::unordered_set<std::string>& tables);

}  // namespace stewkk::sql
