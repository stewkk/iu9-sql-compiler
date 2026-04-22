  struct GenerateIRVisitor {
      llvm::Value* operator()(const BinaryExpression& expr) {
          auto* lhs = std::visit(*this, *expr.lhs);
          auto* rhs = std::visit(*this, *expr.rhs);

          auto* is_null_lhs = CheckNull(lhs);
          auto* is_null_rhs = CheckNull(rhs);
          auto* value_lhs = LoadValue(lhs);
          auto* value_rhs = LoadValue(rhs);

          auto* is_null = builder.CreateOr(is_null_lhs, is_null_rhs);
          llvm::Value* res_value;

          switch (expr.binop) {
              case BinaryOp::kGt:
              {
                  auto* tmp = builder.CreateICmpSGT(value_lhs, value_rhs);
                  res_value = builder.CreateZExt(tmp, builder.getInt64Ty(), "i1_to_i64_zext");
                  break;
              }
            case BinaryOp::kLt:
              {
                  auto* tmp = builder.CreateICmpSLT(value_lhs, value_rhs);
                  res_value = builder.CreateZExt(tmp, builder.getInt64Ty(), "i1_to_i64_zext");
                  break;
              }
            case BinaryOp::kLe:
              {
                  auto* tmp = builder.CreateICmpSLE(value_lhs, value_rhs);
                  res_value = builder.CreateZExt(tmp, builder.getInt64Ty(), "i1_to_i64_zext");
                  break;
              }
            case BinaryOp::kGe:
              {
                  auto* tmp = builder.CreateICmpSGE(value_lhs, value_rhs);
                  res_value = builder.CreateZExt(tmp, builder.getInt64Ty(), "i1_to_i64_zext");
                  break;
              }
...
