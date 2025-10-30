#include <stewkk/sql/logic/llvm_ir_builder.hpp>

#include <string>

namespace stewkk::sql::logic {

Visitor::Visitor()
    : llvm_context_(std::make_unique<llvm::LLVMContext>()),
      ir_builder_(
          std::make_unique<llvm::IRBuilder<llvm::NoFolder>>(*llvm_context_, llvm::NoFolder())),
      ir_module_(std::make_unique<llvm::Module>("sql program", *llvm_context_)) {
  llvm::FunctionType* ft =
    llvm::FunctionType::get(llvm::Type::getInt32Ty(*llvm_context_), {}, false);

  llvm::Function* function = llvm::Function::Create(ft, llvm::Function::ExternalLinkage, "main", ir_module_.get());

  llvm::BasicBlock *bb = llvm::BasicBlock::Create(*llvm_context_, "entry", function);

  ir_builder_->SetInsertPoint(bb);
}

std::any Visitor::visitInt(codegen::TParser::IntContext *ctx) {
    auto i = ctx->INT();
    std::int32_t val = std::stoi(i->getText());
    return static_cast<llvm::Value*>(llvm::ConstantInt::get(*llvm_context_, llvm::APInt(32, val)));
}

std::any Visitor::visitIdent(codegen::TParser::IdentContext *ctx) {
  auto name = ctx->ID()->getText();
  auto alloca = named_values_[name];
  return static_cast<llvm::Value*>(ir_builder_->CreateLoad(alloca->getAllocatedType(), alloca, name));
}

std::any Visitor::visitFlowControl(codegen::TParser::FlowControlContext *ctx) {
  if (ctx->control()->getText() == "if") {
    llvm::Value *cond = std::any_cast<llvm::Value *>(visit(ctx->cond()));
    if (!cond) {
      return nullptr;
    }

    llvm::Function *function = ir_builder_->GetInsertBlock()->getParent();
    llvm::BasicBlock *then_bb = llvm::BasicBlock::Create(*llvm_context_, "then", function);
    llvm::BasicBlock *merge_bb = llvm::BasicBlock::Create(*llvm_context_, "ifcont");
    ir_builder_->CreateCondBr(cond, then_bb, merge_bb);

    ir_builder_->SetInsertPoint(then_bb);

    for (auto stat : ctx->stat()) {
      visit(stat);
    }

    ir_builder_->CreateBr(merge_bb);
    then_bb = ir_builder_->GetInsertBlock();

    function->insert(function->end(), merge_bb);
    ir_builder_->SetInsertPoint(merge_bb);

    return nullptr;
  } else {
    llvm::Function *function = ir_builder_->GetInsertBlock()->getParent();

    llvm::BasicBlock *loop_bb = llvm::BasicBlock::Create(*llvm_context_, "loop", function);
    llvm::BasicBlock *body_bb = llvm::BasicBlock::Create(*llvm_context_, "body", function);
    llvm::BasicBlock *after_bb = llvm::BasicBlock::Create(*llvm_context_, "afterloop", function);
    ir_builder_->CreateBr(loop_bb);
    ir_builder_->SetInsertPoint(loop_bb);

    llvm::Value *cond = std::any_cast<llvm::Value *>(visit(ctx->cond()));
    if (!cond) {
      return nullptr;
    }

    ir_builder_->CreateCondBr(cond, body_bb, after_bb);

    ir_builder_->SetInsertPoint(body_bb);

    for (auto stat : ctx->stat()) {
      visit(stat);
    }

    ir_builder_->CreateBr(loop_bb);

    ir_builder_->SetInsertPoint(after_bb);

    return nullptr;
  }
}

llvm::AllocaInst* Visitor::CreateEntryBlockAlloca(llvm::Function *function,
                                          llvm::StringRef varname) {
  llvm::IRBuilder<> tmp(&function->getEntryBlock(),
                   function->getEntryBlock().begin());
  return tmp.CreateAlloca(llvm::Type::getInt32Ty(*llvm_context_), nullptr, varname);
}

std::any Visitor::visitAssign(codegen::TParser::AssignContext *ctx) {
  auto varname = ctx->ID()->getText();
  std::cerr << "visitAssign " << varname << std::endl;
  llvm::Value* val = std::any_cast<llvm::Value*>(visit(ctx->expr()));
  if (!val) {
    return nullptr;
  }

  std::cerr << "got expr" << std::endl;
  llvm::Function *function = ir_builder_->GetInsertBlock()->getParent();
  llvm::AllocaInst *alloca;
  if (auto it = named_values_.find(varname); it != named_values_.end()) {
    alloca = it->second;
  } else {
    alloca = CreateEntryBlockAlloca(function, varname);
  }
  ir_builder_->CreateStore(val, alloca);

  named_values_[varname] = alloca;

  return nullptr;
}

std::any Visitor::visitCond(codegen::TParser::CondContext *ctx) {
  llvm::Value *lhs = std::any_cast<llvm::Value *>(visit(ctx->lhs));
  llvm::Value *rhs = std::any_cast<llvm::Value *>(visit(ctx->rhs));

  if (!lhs || !rhs) {
    return nullptr;
  }

  auto op = ctx->condOp()->getText();
  if (op == "<") {
    return ir_builder_->CreateICmpSLE(lhs, rhs);
  } else if (op == ">") {
    return ir_builder_->CreateICmpSGE(lhs, rhs);
  } else if (op == "==") {
    return ir_builder_->CreateICmpEQ(lhs, rhs);
  } else if (op == "!=") {
    return ir_builder_->CreateICmpNE(lhs, rhs);
  }
  throw std::logic_error{"invalid binary operator"};
}

std::any Visitor::visitBinaryOp(codegen::TParser::BinaryOpContext *ctx) {
  std::cerr << "binary op" << std::endl;
  llvm::Value *lhs = std::any_cast<llvm::Value *>(visit(ctx->lhs));
  llvm::Value *rhs = std::any_cast<llvm::Value *>(visit(ctx->rhs));

  if (!lhs || !rhs) {
    return nullptr;
  }

  // FIXME: dirty way to get op
  auto op = ctx->op()->getText();
  if (op == "+") {
    return ir_builder_->CreateAdd(lhs, rhs);
  } else if (op == "-") {
    return ir_builder_->CreateSub(lhs, rhs);
  } else if (op == "*") {
    return ir_builder_->CreateMul(lhs, rhs);
  }
  throw std::logic_error{"invalid binary operator"};
}

std::any Visitor::visitReturn(codegen::TParser::ReturnContext *ctx) {
  llvm::Value* val = std::any_cast<llvm::Value*>(visit(ctx->expr()));
  ir_builder_->CreateRet(val);
  return nullptr;
}

const llvm::Module* Visitor::GetIr() const {
    return ir_module_.get();
}

} // namespace stewkk::sql::logic
