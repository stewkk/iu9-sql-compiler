a_expr_qual
    : a_expr_lessless ({this->OnlyAcceptableOps()}? qual_op | )
    ;

a_expr_lessless
    : a_expr_or ((LESS_LESS | GREATER_GREATER) a_expr_or)*
    ;

a_expr_or
    : a_expr_and (OR a_expr_and)*
    ;

a_expr_and
    : a_expr_between (AND a_expr_between)*
    ;

a_expr_between
    : a_expr_in (NOT? BETWEEN SYMMETRIC? a_expr_in AND a_expr_in)?
    ;

a_expr_in
    : a_expr_unary_not (NOT? IN_P in_expr)?
    ;

a_expr_unary_not
    : NOT? a_expr_isnull
    ;

a_expr_isnull
    : a_expr_is_not (ISNULL | NOTNULL)?
    ;

a_expr_is_not
    : a_expr_compare (
        IS NOT? (
            NULL_P
            | TRUE_P
            | FALSE_P
            | UNKNOWN
            | DISTINCT FROM a_expr
            | OF OPEN_PAREN type_list CLOSE_PAREN
            | DOCUMENT_P
            | unicode_normal_form? NORMALIZED
        )
    )?
    ;
