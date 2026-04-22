    | values_clause
    | TABLE relation_expr
    | select_with_parens
    ;

from_clause
    : FROM from_list

    ;

from_list
    : table_ref (COMMA table_ref)*
    ;

table_ref
    : (
        relation_expr alias_clause? tablesample_clause?
        | func_table func_alias_clause?
        | xmltable alias_clause?
        | select_with_parens alias_clause?
        | LATERAL_P (
            xmltable alias_clause?
            | func_table func_alias_clause?
            | select_with_parens alias_clause?
        )
        | OPEN_PAREN table_ref CLOSE_PAREN alias_clause?
    ) (
        CROSS JOIN table_ref
        | NATURAL join_type? JOIN table_ref
        | join_type? JOIN table_ref join_qual
    )*
    ;

join_type
    : INNER_P
    | (FULL | LEFT | RIGHT) OUTER_P?
    ;

join_qual
    : USING OPEN_PAREN name_list CLOSE_PAREN
    | ON a_expr
    ;
relation_expr
    : qualified_name STAR?
    | ONLY (qualified_name | OPEN_PAREN qualified_name CLOSE_PAREN)
    ;

where_clause
    : WHERE a_expr
    ;
