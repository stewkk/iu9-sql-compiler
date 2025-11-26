#pragma once

#include <istream>

#include <stewkk/sql/models/parser/relational_algebra_ast.hpp>

namespace stewkk::sql {

Operator GetAST(std::istream& in);

std::string GetDotRepresentation(const Operator& op);

}  // namespace stewkk::sql
