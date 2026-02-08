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
          mpm.addPass(llvm::createModuleToFunctionPassAdaptor(
                          std::move(fpm)));
          mpm.run(m, mam);
        });
...
