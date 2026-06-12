#pragma once

#include <istream>
#include <optional>

#include <stewkk/sql/models/parser/relational_algebra_ast.hpp>
#include <stewkk/sql/logic/result/result.hpp>
#include <stewkk/sql/logic/optimizer/properties/sort_order.hpp>

namespace stewkk::sql {

struct ParsedQuery {
  Operator op;
  std::optional<SortOrder> required_order;
};

Result<ParsedQuery> GetAST(std::istream& in);

std::string GetDotRepresentation(const Operator& op);

}  // namespace stewkk::sql
