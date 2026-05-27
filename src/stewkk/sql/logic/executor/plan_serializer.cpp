#include <stewkk/sql/logic/executor/plan_serializer.hpp>

#include <cctype>
#include <format>
#include <ranges>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace stewkk::sql {

namespace {

std::string SerializeExpr(const Expression& expr);
std::string SerializeNode(const PhysicalPlanNode& node);

std::string SerializeJoinType(JoinType t) {
    switch (t) {
        case JoinType::kInner: return "Inner";
        case JoinType::kFull:  return "Full";
        case JoinType::kLeft:  return "Left";
        case JoinType::kRight: return "Right";
    }
}

std::string SerializeBinaryOp(BinaryOp op) {
    switch (op) {
        case BinaryOp::kGt:    return ">";
        case BinaryOp::kLt:    return "<";
        case BinaryOp::kLe:    return "<=";
        case BinaryOp::kGe:    return ">=";
        case BinaryOp::kNotEq: return "!=";
        case BinaryOp::kEq:    return "=";
        case BinaryOp::kOr:    return "or";
        case BinaryOp::kAnd:   return "and";
        case BinaryOp::kPlus:  return "+";
        case BinaryOp::kMinus: return "-";
        case BinaryOp::kMul:   return "*";
        case BinaryOp::kDiv:   return "/";
        case BinaryOp::kMod:   return "%";
        case BinaryOp::kPow:   return "^";
    }
}

std::string SerializeUnaryOp(UnaryOp op) {
    switch (op) {
        case UnaryOp::kNot:   return "not";
        case UnaryOp::kMinus: return "uminus";
        case UnaryOp::kIsNull: return "isnull";
    }
}

std::string SerializeExpr(const Expression& expr) {
    struct Visitor {
        std::string operator()(const BinaryExpression& e) const {
            return std::format("({} {} {})", SerializeBinaryOp(e.binop),
                               SerializeExpr(*e.lhs), SerializeExpr(*e.rhs));
        }
        std::string operator()(const Attribute& a) const {
            return std::format("(attr {} {})", a.table, a.name);
        }
        std::string operator()(IntConst n) const {
            return std::to_string(n);
        }
        std::string operator()(const UnaryExpression& e) const {
            return std::format("({} {})", SerializeUnaryOp(e.op), SerializeExpr(*e.child));
        }
        std::string operator()(Literal l) const {
            switch (l) {
                case Literal::kNull:    return "NULL";
                case Literal::kTrue:    return "TRUE";
                case Literal::kFalse:   return "FALSE";
                case Literal::kUnknown: return "UNKNOWN";
            }
        }
    };
    return std::visit(Visitor{}, expr);
}

std::string SerializeNode(const PhysicalPlanNode& node) {
    struct Visitor {
        std::string operator()(const SeqScan& n) const {
            return std::format("(SeqScan {})", n.table);
        }
        std::string operator()(const PhysicalFilter& n) const {
            return std::format("(PhysicalFilter {} {})",
                               SerializeExpr(n.predicate), SerializeNode(*n.source));
        }
        std::string operator()(const PhysicalProjection& n) const {
            std::string exprs;
            for (const auto& e : n.expressions) {
                if (!exprs.empty()) exprs += ' ';
                exprs += SerializeExpr(e);
            }
            return std::format("(PhysicalProjection (exprs {}) {})",
                               exprs, SerializeNode(*n.source));
        }
        std::string operator()(const NestedLoopCrossJoin& n) const {
            return std::format("(NestedLoopCrossJoin {} {})",
                               SerializeNode(*n.lhs), SerializeNode(*n.rhs));
        }
        std::string operator()(const NestedLoopJoin& n) const {
            return std::format("(NestedLoopJoin {} {} {} {})",
                               SerializeJoinType(n.type), SerializeExpr(n.qual),
                               SerializeNode(*n.lhs), SerializeNode(*n.rhs));
        }
        std::string operator()(const IndexSeek& n) const {
            return std::format("(IndexSeek {} {})", SerializeExpr(n.predicate), n.table);
        }
        std::string operator()(const HashJoin& n) const {
            return std::format("(HashJoin {} {} {} {})",
                               SerializeJoinType(n.type), SerializeExpr(n.qual),
                               SerializeNode(*n.lhs), SerializeNode(*n.rhs));
        }
        std::string operator()(const MergeJoin& n) const {
            return std::format("(MergeJoin {} {} {} {})",
                               SerializeJoinType(n.type), SerializeExpr(n.qual),
                               SerializeNode(*n.lhs), SerializeNode(*n.rhs));
        }
        std::string operator()(const PhysicalSort& n) const {
            std::string keys;
            for (const auto& k : n.keys.keys) {
                if (!keys.empty()) keys += ' ';
                keys += k.table;
                keys += '.';
                keys += k.column;
                keys += k.dir == Direction::kAsc ? " Asc" : " Desc";
            }
            return std::format("(Sort (keys {}) {})", keys, SerializeNode(*n.source));
        }
    };
    return std::visit(Visitor{}, node);
}

enum class TokenKind { LParen, RParen, Atom };

struct Token {
    TokenKind kind;
    std::string text;
};

std::vector<Token> Tokenize(std::string_view input) {
    std::vector<Token> tokens;
    size_t i = 0;
    while (i < input.size()) {
        if (std::isspace(static_cast<unsigned char>(input[i]))) { ++i; continue; }
        if (input[i] == '(') { tokens.push_back({TokenKind::LParen, "("}); ++i; continue; }
        if (input[i] == ')') { tokens.push_back({TokenKind::RParen, ")"}); ++i; continue; }
        size_t j = i;
        while (j < input.size()
               && !std::isspace(static_cast<unsigned char>(input[j]))
               && input[j] != '(' && input[j] != ')')
            ++j;
        tokens.push_back({TokenKind::Atom, std::string(input.substr(i, j - i))});
        i = j;
    }
    return tokens;
}

struct ParseState {
    const std::vector<Token>& tokens;
    size_t pos = 0;

    const Token& Peek() const {
        if (pos >= tokens.size())
            throw std::runtime_error("unexpected end of input");
        return tokens[pos];
    }

    const Token& Consume() {
        if (pos >= tokens.size())
            throw std::runtime_error("unexpected end of input");
        return tokens[pos++];
    }

    void ExpectLParen() {
        const auto& t = Consume();
        if (t.kind != TokenKind::LParen)
            throw std::runtime_error(std::format("expected '(' but got '{}'", t.text));
    }

    void ExpectRParen() {
        const auto& t = Consume();
        if (t.kind != TokenKind::RParen)
            throw std::runtime_error(std::format("expected ')' but got '{}'", t.text));
    }

    std::string ExpectAtom() {
        const auto& t = Consume();
        if (t.kind != TokenKind::Atom)
            throw std::runtime_error(std::format("expected atom but got '{}'", t.text));
        return t.text;
    }
};

const std::unordered_map<std::string, BinaryOp> kBinaryOps = {
    {">", BinaryOp::kGt},    {"<", BinaryOp::kLt},   {"<=", BinaryOp::kLe},
    {">=", BinaryOp::kGe},   {"!=", BinaryOp::kNotEq}, {"=", BinaryOp::kEq},
    {"or", BinaryOp::kOr},   {"and", BinaryOp::kAnd}, {"+", BinaryOp::kPlus},
    {"-", BinaryOp::kMinus}, {"*", BinaryOp::kMul},   {"/", BinaryOp::kDiv},
    {"%", BinaryOp::kMod},   {"^", BinaryOp::kPow},
};

const std::unordered_map<std::string, UnaryOp> kUnaryOps = {
    {"not", UnaryOp::kNot}, {"uminus", UnaryOp::kMinus}, {"isnull", UnaryOp::kIsNull},
};

const std::unordered_map<std::string, JoinType> kJoinTypes = {
    {"Inner", JoinType::kInner}, {"Full", JoinType::kFull},
    {"Left", JoinType::kLeft},   {"Right", JoinType::kRight},
};

const std::unordered_map<std::string, Literal> kLiterals = {
    {"NULL", Literal::kNull}, {"TRUE", Literal::kTrue},
    {"FALSE", Literal::kFalse}, {"UNKNOWN", Literal::kUnknown},
};

Expression ParseExpr(ParseState& s);
PhysicalPlanNode ParseNode(ParseState& s);

Expression ParseExpr(ParseState& s) {
    const auto& t = s.Peek();

    if (t.kind == TokenKind::Atom) {
        auto text = s.ExpectAtom();
        try {
            size_t idx;
            IntConst n = std::stoll(text, &idx);
            if (idx == text.size()) return n;
        } catch (...) {}
        if (auto it = kLiterals.find(text); it != kLiterals.end()) return it->second;
        throw std::runtime_error(std::format("unexpected atom in expression: '{}'", text));
    }

    if (t.kind == TokenKind::LParen) {
        s.ExpectLParen();
        auto head = s.ExpectAtom();

        if (head == "attr") {
            auto table = s.ExpectAtom();
            auto name  = s.ExpectAtom();
            s.ExpectRParen();
            return Attribute{std::move(table), std::move(name)};
        }
        if (auto it = kBinaryOps.find(head); it != kBinaryOps.end()) {
            auto lhs = ParseExpr(s);
            auto rhs = ParseExpr(s);
            s.ExpectRParen();
            return BinaryExpression{
                std::make_shared<Expression>(std::move(lhs)),
                it->second,
                std::make_shared<Expression>(std::move(rhs)),
            };
        }
        if (auto it = kUnaryOps.find(head); it != kUnaryOps.end()) {
            auto child = ParseExpr(s);
            s.ExpectRParen();
            return UnaryExpression{it->second, std::make_shared<Expression>(std::move(child))};
        }
        throw std::runtime_error(std::format("unknown expression head: '{}'", head));
    }

    throw std::runtime_error("expected expression");
}

PhysicalPlanNode ParseNode(ParseState& s) {
    s.ExpectLParen();
    auto head = s.ExpectAtom();

    if (head == "SeqScan") {
        auto table = s.ExpectAtom();
        s.ExpectRParen();
        return SeqScan{std::move(table)};
    }
    if (head == "PhysicalFilter") {
        auto pred   = ParseExpr(s);
        auto source = ParseNode(s);
        s.ExpectRParen();
        return PhysicalFilter{
            std::make_shared<PhysicalPlanNode>(std::move(source)),
            std::move(pred),
        };
    }
    if (head == "PhysicalProjection") {
        s.ExpectLParen();
        auto kw = s.ExpectAtom();
        if (kw != "exprs")
            throw std::runtime_error(std::format("expected 'exprs' but got '{}'", kw));
        std::vector<Expression> exprs;
        while (s.Peek().kind != TokenKind::RParen)
            exprs.push_back(ParseExpr(s));
        s.ExpectRParen();
        auto source = ParseNode(s);
        s.ExpectRParen();
        return PhysicalProjection{
            std::make_shared<PhysicalPlanNode>(std::move(source)),
            std::move(exprs),
        };
    }
    if (head == "NestedLoopCrossJoin") {
        auto lhs = ParseNode(s);
        auto rhs = ParseNode(s);
        s.ExpectRParen();
        return NestedLoopCrossJoin{
            std::make_shared<PhysicalPlanNode>(std::move(lhs)),
            std::make_shared<PhysicalPlanNode>(std::move(rhs)),
        };
    }
    auto ParseJoinNode = [&]<typename JoinT>() -> JoinT {
        auto type_str = s.ExpectAtom();
        auto it = kJoinTypes.find(type_str);
        if (it == kJoinTypes.end())
            throw std::runtime_error(std::format("unknown join type: '{}'", type_str));
        auto qual = ParseExpr(s);
        auto lhs  = ParseNode(s);
        auto rhs  = ParseNode(s);
        s.ExpectRParen();
        return JoinT{
            std::make_shared<PhysicalPlanNode>(std::move(lhs)),
            std::make_shared<PhysicalPlanNode>(std::move(rhs)),
            it->second,
            std::move(qual),
        };
    };

    if (head == "IndexSeek") {
        auto pred  = ParseExpr(s);
        auto table = s.ExpectAtom();
        s.ExpectRParen();
        return IndexSeek{std::move(table), std::move(pred)};
    }
    if (head == "NestedLoopJoin") return ParseJoinNode.template operator()<NestedLoopJoin>();
    if (head == "HashJoin")       return ParseJoinNode.template operator()<HashJoin>();
    if (head == "MergeJoin")      return ParseJoinNode.template operator()<MergeJoin>();
    if (head == "Sort") {
        s.ExpectLParen();
        auto kw = s.ExpectAtom();
        if (kw != "keys")
            throw std::runtime_error(std::format("expected 'keys' but got '{}'", kw));
        std::vector<SortKey> keys;
        while (s.Peek().kind != TokenKind::RParen) {
            auto qualified = s.ExpectAtom();
            auto dot = qualified.find('.');
            if (dot == std::string::npos)
                throw std::runtime_error("sort key must be table.column, got: " + qualified);
            std::string table = qualified.substr(0, dot);
            std::string col = qualified.substr(dot + 1);
            auto dir_str = s.ExpectAtom();
            Direction dir = dir_str == "Asc" ? Direction::kAsc : Direction::kDesc;
            keys.push_back({std::move(table), std::move(col), dir});
        }
        s.ExpectRParen();
        auto source = ParseNode(s);
        s.ExpectRParen();
        return PhysicalSort{
            std::make_shared<PhysicalPlanNode>(std::move(source)),
            SortOrder{std::move(keys)},
        };
    }

    throw std::runtime_error(std::format("unknown plan node: '{}'", head));
}

std::string EscapeLabel(std::string_view s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        if (c == '"' || c == '\\') out += '\\';
        out += c;
    }
    return out;
}

struct DotBuilder {
    std::ostringstream os;
    int next_id = 0;

    int Emit(std::string_view label) {
        int id = next_id++;
        os << std::format("  n{} [label=\"{}\"]\n", id, EscapeLabel(label));
        return id;
    }

    void EmitEdge(int from, int to) {
        os << std::format("  n{} -> n{}\n", from, to);
    }

    int operator()(const SeqScan& n) {
        return Emit(std::format("SeqScan\\n{}", n.table));
    }
    int operator()(const PhysicalFilter& n) {
        int src = std::visit(*this, *n.source);
        int id = Emit(std::format("σ {}", ToString(n.predicate)));
        EmitEdge(src, id);
        return id;
    }
    int operator()(const PhysicalProjection& n) {
        int src = std::visit(*this, *n.source);
        auto exprs = n.expressions
                     | std::views::transform([](const Expression& e) { return ToString(e); })
                     | std::views::join_with(std::string(", "))
                     | std::ranges::to<std::string>();
        int id = Emit(std::format("π {}", exprs));
        EmitEdge(src, id);
        return id;
    }
    int operator()(const NestedLoopCrossJoin& n) {
        int lhs = std::visit(*this, *n.lhs);
        int rhs = std::visit(*this, *n.rhs);
        int id = Emit("×");
        EmitEdge(lhs, id);
        EmitEdge(rhs, id);
        return id;
    }
    int operator()(const NestedLoopJoin& n) {
        int lhs = std::visit(*this, *n.lhs);
        int rhs = std::visit(*this, *n.rhs);
        int id = Emit(std::format("NL {}\\nON {}", ToString(n.type), ToString(n.qual)));
        EmitEdge(lhs, id);
        EmitEdge(rhs, id);
        return id;
    }
    int operator()(const IndexSeek& n) {
        return Emit(std::format("IndexSeek\\n{}\\n{}", n.table, ToString(n.predicate)));
    }
    int operator()(const HashJoin& n) {
        int lhs = std::visit(*this, *n.lhs);
        int rhs = std::visit(*this, *n.rhs);
        int id = Emit(std::format("Hash {}\\nON {}", ToString(n.type), ToString(n.qual)));
        EmitEdge(lhs, id);
        EmitEdge(rhs, id);
        return id;
    }
    int operator()(const MergeJoin& n) {
        int lhs = std::visit(*this, *n.lhs);
        int rhs = std::visit(*this, *n.rhs);
        int id = Emit(std::format("Merge {}\\nON {}", ToString(n.type), ToString(n.qual)));
        EmitEdge(lhs, id);
        EmitEdge(rhs, id);
        return id;
    }
    int operator()(const PhysicalSort& n) {
        int src = std::visit(*this, *n.source);
        std::string keys;
        for (const auto& k : n.keys.keys) {
            if (!keys.empty()) keys += ", ";
            keys += k.table;
            keys += '.';
            keys += k.column;
            keys += k.dir == Direction::kAsc ? " asc" : " desc";
        }
        int id = Emit(std::format("Sort\\n{}", keys));
        EmitEdge(src, id);
        return id;
    }
};

} // namespace

std::string Serialize(const PhysicalPlanNode& node) {
    return SerializeNode(node);
}

PhysicalPlanNode Deserialize(std::string_view text) {
    auto tokens = Tokenize(text);
    ParseState s{tokens};
    return ParseNode(s);
}

std::string SerializeDot(const PhysicalPlanNode& node) {
    DotBuilder b;
    std::visit(b, node);
    return std::format("digraph G {{ rankdir=BT;\n{}}}\n", b.os.str());
}

} // namespace stewkk::sql
