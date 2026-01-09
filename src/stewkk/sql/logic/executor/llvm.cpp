#include <stewkk/sql/logic/executor/llvm.hpp>

#include <iostream>

#include <llvm/Support/TargetSelect.h>
#include <llvm/IR/IRBuilder.h>
#include <boost/asio/bind_executor.hpp>
#include <llvm/Passes/PassBuilder.h>
#include <llvm/Transforms/InstCombine/InstCombine.h>
#include <llvm/Transforms/Scalar/Reassociate.h>
#include <llvm/Transforms/Scalar/GVN.h>
#include <llvm/Transforms/Scalar/SimplifyCFG.h>
#include <llvm/Transforms/Scalar/DCE.h>
#include <llvm/Transforms/Scalar/ADCE.h>
#include <llvm/Transforms/Scalar/SROA.h>
#include <llvm/Transforms/Scalar/MemCpyOptimizer.h>
#include <llvm/Transforms/Scalar/EarlyCSE.h>
#include <llvm/IR/PassManager.h>

namespace stewkk::sql {

JITCompiler::JITCompiler(boost::asio::any_io_executor executor) : jit_strand_(executor) {
  llvm::InitializeNativeTarget();
  llvm::InitializeNativeTargetAsmPrinter();
  llvm::InitializeNativeTargetAsmParser();

  auto jit_or_error = llvm::orc::LLJITBuilder().create();
  if (!jit_or_error) {
    throw std::runtime_error("failed to create llvm::LLJIT");
  }
  jit_ = std::move(*jit_or_error);

  jit_->getIRTransformLayer().setTransform(
      [](llvm::orc::ThreadSafeModule tsm, llvm::orc::MaterializationResponsibility& r)
          -> llvm::Expected<llvm::orc::ThreadSafeModule> {
        tsm.withModuleDo([](llvm::Module& m) {
#ifdef DEBUG
          std::clog << "IR before optimization:\n";
          m.print(llvm::errs(), nullptr);
#endif
          llvm::LoopAnalysisManager lam;
          llvm::FunctionAnalysisManager fam;
          llvm::CGSCCAnalysisManager cgam;
          llvm::ModuleAnalysisManager mam;

          llvm::PassBuilder pb;

          pb.registerModuleAnalyses(mam);
          pb.registerCGSCCAnalyses(cgam);
          pb.registerFunctionAnalyses(fam);
          pb.registerLoopAnalyses(lam);
          pb.crossRegisterProxies(lam, fam, cgam, mam);

          llvm::ModulePassManager mpm;
          llvm::FunctionPassManager fpm;

          fpm.addPass(llvm::EarlyCSEPass(true));
          fpm.addPass(llvm::SROAPass(llvm::SROAOptions::ModifyCFG));

          fpm.addPass(llvm::InstCombinePass());
          fpm.addPass(llvm::SimplifyCFGPass());

          fpm.addPass(llvm::ReassociatePass());
          fpm.addPass(llvm::GVNPass());
          fpm.addPass(llvm::MemCpyOptPass());

          fpm.addPass(llvm::SimplifyCFGPass());
          fpm.addPass(llvm::InstCombinePass());
          fpm.addPass(llvm::DCEPass());
          fpm.addPass(llvm::ADCEPass());
          
          // TODO: SIMD
          //fpm.addPass(llvm::SLPVectorizerPass());
          //fpm.addPass(llvm::LoopVectorizePass());

          mpm.addPass(llvm::createModuleToFunctionPassAdaptor(std::move(fpm)));

          mpm.run(m, mam);

#ifdef DEBUG
          std::clog << "IR after optimization:\n";
          m.print(llvm::errs(), nullptr);
#endif
        });
        return std::move(tsm);
      });
}

boost::asio::awaitable<std::pair<JITCompiler::CompiledExpression, llvm::orc::ResourceTrackerSP>>
JITCompiler::CompileExpression(const Expression& expr, const AttributesInfo& attrs) {
  boost::asio::any_io_executor current_executor = co_await boost::asio::this_coro::executor;
  co_await boost::asio::dispatch(boost::asio::bind_executor(jit_strand_, boost::asio::deferred));

  auto resource_tracker = jit_->getMainJITDylib().createResourceTracker();

  auto ctx = std::make_unique<llvm::LLVMContext>();

  auto module_name = std::format("expr_module_{}", id_.fetch_add(1));
  auto llvm_module = std::make_unique<llvm::Module>(std::move(module_name), *ctx);
  llvm_module->setDataLayout(jit_->getDataLayout());
  llvm::orc::ThreadSafeModule tsm(std::move(llvm_module), std::move(ctx));

  std::string func_name;
  tsm.withModuleDo([&](llvm::Module& m) {
    auto* func = GenerateIR(m, expr, attrs);
    func_name = func->getName().str();
  });

  auto err = jit_->addIRModule(resource_tracker, std::move(tsm));
  if (err) {
    throw std::runtime_error("failed to add IR module");
  }

  auto symbol = jit_->lookup(func_name);
  if (!symbol) {
    throw std::runtime_error("lookup failed");
  }

  auto* compiled_expression = symbol->toPtr<CompiledExpression>();
  co_await boost::asio::dispatch(boost::asio::bind_executor(current_executor, boost::asio::deferred));
  co_return std::make_pair(compiled_expression, std::move(resource_tracker));
}

llvm::Function* JITCompiler::GenerateIR(
    llvm::Module& llvm_module,
    const Expression& expr,
    const AttributesInfo& attrs) {

  llvm::IRBuilder<> builder(llvm_module.getContext());

  auto* value_type = llvm::StructType::create(
      llvm_module.getContext(),
      {builder.getInt8Ty(), builder.getInt64Ty()},
      "Value");
  
  auto* tuple_type = llvm::PointerType::getUnqual(value_type);
  auto* attrs_type = llvm::PointerType::getUnqual(
      llvm::StructType::get(llvm_module.getContext()));

  auto* result_ptr_type = llvm::PointerType::getUnqual(value_type);
  auto* func_type = llvm::FunctionType::get(
      llvm::Type::getVoidTy(llvm_module.getContext()),
      {result_ptr_type, tuple_type, attrs_type}, false);
  auto func_name = std::format("eval_expr_{}", id_.fetch_add(1));
  auto* func = llvm::Function::Create(
      func_type, llvm::Function::ExternalLinkage, func_name, &llvm_module);

  func->addParamAttr(0, llvm::Attribute::NoAlias);

  auto* entry = llvm::BasicBlock::Create(llvm_module.getContext(), "entry", func);
  builder.SetInsertPoint(entry);

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
                  llvm::Type* i64_type = llvm::Type::getInt64Ty(builder.getContext());
                  res_value = builder.CreateZExt(tmp, i64_type, "i1_to_i64_zext");
                  break;
              }
            case BinaryOp::kLt:
              {
                  auto* tmp = builder.CreateICmpSLT(value_lhs, value_rhs);
                  llvm::Type* i64_type = llvm::Type::getInt64Ty(builder.getContext());
                  res_value = builder.CreateZExt(tmp, i64_type, "i1_to_i64_zext");
                  break;
              }
            case BinaryOp::kLe:
              {
                  auto* tmp = builder.CreateICmpSLE(value_lhs, value_rhs);
                  llvm::Type* i64_type = llvm::Type::getInt64Ty(builder.getContext());
                  res_value = builder.CreateZExt(tmp, i64_type, "i1_to_i64_zext");
                  break;
              }
            case BinaryOp::kGe:
              {
                  auto* tmp = builder.CreateICmpSGE(value_lhs, value_rhs);
                  llvm::Type* i64_type = llvm::Type::getInt64Ty(builder.getContext());
                  res_value = builder.CreateZExt(tmp, i64_type, "i1_to_i64_zext");
                  break;
              }
            case BinaryOp::kNotEq:
              {
                  auto* tmp = builder.CreateICmpNE(value_lhs, value_rhs);
                  llvm::Type* i64_type = llvm::Type::getInt64Ty(builder.getContext());
                  res_value = builder.CreateZExt(tmp, i64_type, "i1_to_i64_zext");
                  break;
              }
            case BinaryOp::kEq:
              {
                  auto* tmp = builder.CreateICmpEQ(value_lhs, value_rhs);
                  llvm::Type* i64_type = llvm::Type::getInt64Ty(builder.getContext());
                  res_value = builder.CreateZExt(tmp, i64_type, "i1_to_i64_zext");
                  break;
              }
            case BinaryOp::kOr:
                throw std::logic_error{"or is not supported in llvm codegen"};
            case BinaryOp::kAnd:
                throw std::logic_error{"and is not supported in llvm codegen"};
            case BinaryOp::kPlus:
                  res_value = builder.CreateAdd(value_lhs, value_rhs);
                  break;
            case BinaryOp::kMinus:
                  res_value = builder.CreateSub(value_lhs, value_rhs);
                  break;
            case BinaryOp::kMul:
                  res_value = builder.CreateMul(value_lhs, value_rhs);
                  break;
            case BinaryOp::kDiv:
                // TODO: handle div by zero
                  res_value = builder.CreateSDiv(value_lhs, value_rhs);
                  break;
            case BinaryOp::kMod:
                // TODO: handle div by zero
                  res_value = builder.CreateSRem(value_lhs, value_rhs);
                  break;
            case BinaryOp::kPow:
                throw std::logic_error{"pow is not supported in llvm codegen"};
          }

          auto* is_null_i8 = builder.CreateZExt(is_null, builder.getInt8Ty(), "is_null_i8");
          
          llvm::Value* value = llvm::UndefValue::get(value_type);
          value = builder.CreateInsertValue(value, is_null_i8, {0}, "result.with_is_null");
          value = builder.CreateInsertValue(value, res_value, {1}, "result");

          return value;
      }
      llvm::Value* operator()(const UnaryExpression& expr) {
          auto* child = std::visit(*this, *expr.child);
          switch (expr.op) {
              case UnaryOp::kNot: {
                auto is_null = CheckNull(child);
                auto value = LoadValue(child);

                auto* is_zero = builder.CreateICmpEQ(
                    value, builder.getIntN(value->getType()->getIntegerBitWidth(), 0), "is_zero");
                auto* logical_not
                    = builder.CreateZExt(is_zero, value->getType(), "logical_not_result");

                auto* select = builder.CreateSelect(
                    is_null,
                    llvm::ConstantStruct::get(static_cast<llvm::StructType*>(value_type),
                                              {builder.getInt8(1), builder.getInt64(0)}),
                    logical_not);
                return select;
              }
              case UnaryOp::kMinus: {
                auto is_null = CheckNull(child);
                auto value = LoadValue(child);
                
                auto* negated = builder.CreateNeg(value);
                
                auto* select = builder.CreateSelect(
                    is_null,
                    llvm::ConstantStruct::get(static_cast<llvm::StructType*>(value_type),
                                              {builder.getInt8(1), builder.getInt64(0)}),
                    negated);
                return select;
              }
          }
      }
      llvm::Value* operator()(const IntConst& expr) {
        return llvm::ConstantStruct::get(static_cast<llvm::StructType*>(value_type),
                                  {builder.getInt8(0), builder.getInt64(expr)});
      }
      llvm::Value* operator()(const Literal& expr) {
          switch (expr) {
            case Literal::kNull:
              return
                  llvm::ConstantStruct::get(static_cast<llvm::StructType*>(value_type),
                                            {builder.getInt8(1), builder.getInt64(0)});
            case Literal::kTrue:
              return
                  llvm::ConstantStruct::get(static_cast<llvm::StructType*>(value_type),
                                            {builder.getInt8(0), builder.getInt64(1)});
            case Literal::kFalse:
              return
                  llvm::ConstantStruct::get(static_cast<llvm::StructType*>(value_type),
                                            {builder.getInt8(0), builder.getInt64(0)});
            case Literal::kUnknown:
              return
                  llvm::ConstantStruct::get(static_cast<llvm::StructType*>(value_type),
                                            {builder.getInt8(1), builder.getInt64(0)});
          }
      }
      llvm::Value* operator()(const Attribute& expr) {
        auto it = std::find_if(
            attrs.begin(), attrs.end(), [&expr](const AttributeInfo& attr_info) {
              return attr_info.name == expr.name && attr_info.table == expr.table;
            });
        // NOTE: already checked in GetExpressionType
        auto index = it - attrs.begin();
        auto* index_const = llvm::ConstantInt::get(llvm::Type::getInt32Ty(llvm_module.getContext()), index);

        auto* struct_ptr = builder.CreateInBoundsGEP(
            value_type,
            tuples_arg,
            index_const,
            "struct_ptr");

        auto* loaded_struct = builder.CreateLoad(
            value_type,
            struct_ptr,
            "loaded_struct");

        return loaded_struct;
      }

      llvm::Value* LoadIsNull(llvm::Value* v) {
        return builder.CreateExtractValue(v, {0}, "value.is_null");
      }

      llvm::Value* LoadValue(llvm::Value* v) {
        return builder.CreateExtractValue(v, {1}, "value.value");
      }

      llvm::Value* CheckNull(llvm::Value* v) {
          auto* is_null_value = LoadIsNull(v);
          return builder.CreateICmpNE(
              is_null_value, builder.getInt8(0), "is_null");
      }

      llvm::Module& llvm_module;
      llvm::IRBuilder<>& builder;
      llvm::StructType* value_type;
      const AttributesInfo& attrs;
      llvm::Value* tuples_arg;
  };

  auto* result_value = std::visit(
      GenerateIRVisitor{llvm_module, builder, value_type, attrs, func->getArg(1)}, expr);
  auto* result_ptr = func->getArg(0);
  builder.CreateStore(result_value, result_ptr);
  builder.CreateRetVoid();

  return func;
}

} // namespace stewkk::sql
