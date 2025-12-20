#pragma once

#include <memory>
#include <atomic>

#include <llvm/ExecutionEngine/Orc/LLJIT.h>
#include <boost/asio/strand.hpp>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>

#include <stewkk/sql/models/executor/tuple.hpp>
#include <stewkk/sql/models/parser/relational_algebra_ast.hpp>

namespace stewkk::sql {

class JITCompiler {
    public:
        using CompiledExpression = void (*)(Value*, const Value*, const AttributeInfo*);

        explicit JITCompiler(boost::asio::any_io_executor executor);

        boost::asio::awaitable<std::pair<CompiledExpression, llvm::orc::ResourceTrackerSP>>
        CompileExpression(const Expression& expr, const AttributesInfo& attrs);

    private:
      llvm::Function* GenerateIR(llvm::Module& llvm_module, const Expression& expr,
                                 const AttributesInfo& attrs);

    private:
        std::unique_ptr<llvm::orc::LLJIT> jit_;
        std::atomic<uint64_t> id_;
        boost::asio::strand<boost::asio::any_io_executor> jit_strand_;
};

} // namespace stewkk::sql
