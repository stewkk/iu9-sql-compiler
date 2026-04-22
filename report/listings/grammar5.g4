c_expr
    : EXISTS select_with_parens               # c_expr_exists
    | ARRAY (select_with_parens | array_expr) # c_expr_expr
    | PARAM opt_indirection                   # c_expr_expr
    | GROUPING OPEN_PAREN expr_list CLOSE_PAREN # c_expr_expr
    | /*22*/ UNIQUE select_with_parens        # c_expr_expr
    | columnref                               # c_expr_expr
    | aexprconst                              # c_expr_expr
    | OPEN_PAREN a_expr_in_parens = a_expr
      CLOSE_PAREN opt_indirection # c_expr_expr
    | case_expr                               # c_expr_case
    | func_expr                               # c_expr_expr
    | select_with_parens indirection?         # c_expr_expr
    | explicit_row                            # c_expr_expr
    | implicit_row                            # c_expr_expr
    | row OVERLAPS row /* 14*/                # c_expr_expr
    | DEFAULT                                 # c_expr_expr
    ;

columnref
    : colid indirection?
    ;

target_list
    : target_el (COMMA target_el)*
    ;

target_el
    : a_expr (AS colLabel | bareColLabel |) # target_label
    | STAR                                # target_star
    ;

qualified_name
    : colid indirection?
    ;

aexprconst
    : iconst
    | TRUE_P
    | FALSE_P
    | NULL_P
    ;

iconst
    : Integral
    | BinaryIntegral
    | OctalIntegral
    | HexadecimalIntegral
    ;
