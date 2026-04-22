#pragma once

#include <istream>

#include <stewkk/sql/models/parser/relational_algebra_ast.hpp>
#include <stewkk/sql/logic/result/result.hpp>

namespace stewkk::sql {

Result<Operator> GetAST(std::istream& in);

std::string GetDotRepresentation(const Operator& op);

}  // namespace stewkk::sql
