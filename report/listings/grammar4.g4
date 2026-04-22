a_expr_compare
    : a_expr_like (
        (LT | GT | EQUAL | LESS_EQUALS | GREATER_EQUALS | NOT_EQUALS) a_expr_like
        | subquery_Op sub_type (select_with_parens | OPEN_PAREN a_expr CLOSE_PAREN) /*21*/
    )?
    ;

a_expr_like
    : a_expr_qual_op (NOT? (LIKE | ILIKE | SIMILAR TO) a_expr_qual_op escape_?)?
    ;

a_expr_qual_op
    : a_expr_unary_qualop (qual_op a_expr_unary_qualop)*
    ;

a_expr_unary_qualop
    : qual_op? a_expr_add
    ;

a_expr_add
    : a_expr_mul ((MINUS | PLUS) a_expr_mul)*
    ;

a_expr_mul
    : a_expr_caret ((STAR | SLASH | PERCENT) a_expr_caret)*
    ;

a_expr_caret
    : a_expr_unary_sign (CARET a_expr_unary_sign)?
    ;

a_expr_unary_sign
    : (MINUS | PLUS)? a_expr_at_time_zone /* */
    ;

a_expr_at_time_zone
    : a_expr_collate (AT TIME ZONE a_expr)?
    ;

a_expr_collate
    : a_expr_typecast (COLLATE any_name)?
    ;

a_expr_typecast
    : c_expr (TYPECAST typename)*
    ;
