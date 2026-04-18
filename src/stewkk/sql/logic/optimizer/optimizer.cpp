#include <stewkk/sql/logic/optimizer/optimizer.hpp>

namespace stewkk::sql {

Operator Optimizer::Optimize(Operator expression) {
    return expression;
}

/*
** нам нужно как-то маппить логическое выражение в его группу.
** для этого нужно хотя бы научиться проверять эквивалентны ли выражения.
** 
*/

}  // namespace stewkk::sql
