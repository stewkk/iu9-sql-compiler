#pragma once

#include <stewkk/sql/logic/optimizer/rule.hpp>
#include <stewkk/sql/logic/optimizer/schema_catalog.hpp>

namespace stewkk::sql {

class ImplementIndexSeek : public ImplementationRule {
public:
  explicit ImplementIndexSeek(IndexCatalog indexes);

  bool IsApplicable(utils::NotNull<LogicalExpr*> expr) override;
  std::vector<utils::NotNull<PhysicalExpr*>> Apply(utils::NotNull<LogicalExpr*> expr, Memo& memo) override;

private:
  IndexCatalog indexes_;
};

bool HasCompatibleIndexSeek(const logical::Filter& filter, const IndexCatalog& indexes);

}  // namespace stewkk::sql
