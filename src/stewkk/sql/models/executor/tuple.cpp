#include <stewkk/sql/models/executor/tuple.hpp>

#include <sstream>
#include <ranges>

namespace stewkk::sql {

NonNullValue GetTrileanValue(Trilean v) {
    NonNullValue res;
    res.trilean_value = v;
    return res;
}

std::string ToString(Trilean v) {
    switch (v) {
      case Trilean::kTrue:
          return "TRUE    ";
      case Trilean::kFalse:
          return "FALSE   ";
      case Trilean::kUnknown:
          return "UNKNOWN ";
    }
}

std::string ToString(const Relation& relation) {
    struct FormatVisitor {
        void operator()(const NullValue& v) {
            s << "NULL    ";
        }
        void operator()(const NonNullValue& v) {
            if (attr.type == Type::kInt) {
                s << std::format("{:<8}", v.int_value);
                return;
            }
            s << ToString(v.trilean_value) << ' ';
        }

        std::ostringstream& s;
        const AttributeInfo& attr;
    };

    std::ostringstream s;
    for (const auto& tuple : relation.tuples) {
        for (const auto& [val, attr] : std::views::zip(tuple, relation.attributes)) {
            std::visit(FormatVisitor{s, attr}, val);
        }
        s << '\n';
    }

    return s.str();
}

}  // namespace stewkk::sql
