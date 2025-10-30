parser grammar TParser;

options {
    tokenVocab = TLexer;
}

main: stat+ EOF;

stat: ID Assign expr Semicolon # Assign
    | Return expr Semicolon # Return
    | Break Semicolon # Break
    | Continue Semicolon # Continue
    | control OpenPar cond ClosePar OpenCurly stat+ CloseCurly # FlowControl
;

control: If
    | While
;

cond: lhs=expr condOp rhs=expr;

condOp: Equal
    | NotEqual
    | LessThan
    | GreaterThan
;

expr: lhs=expr op rhs=expr # BinaryOp
    | OpenPar expr ClosePar # Nested
    | identifier = ID # Ident
    | INT # Int
;

op: Star
    | Minus
    | Plus
;
