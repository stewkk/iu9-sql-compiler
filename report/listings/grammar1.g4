root
    : stmtmulti EOF
    ;

stmt
    : selectstmt
    | ...
    ;

stmtmulti
    : stmt? (SEMI stmt?)*
    ;

select_with_parens
    : OPEN_PAREN select_no_parens CLOSE_PAREN
    | OPEN_PAREN select_with_parens CLOSE_PAREN
    ;

select_no_parens
    : select_clause sort_clause_? (
        for_locking_clause select_limit_?
        | select_limit for_locking_clause_?
    )?
    | with_clause select_clause sort_clause_? (
        for_locking_clause select_limit_?
        | select_limit for_locking_clause_?
    )?
    ;

select_clause
    : simple_select_intersect ((UNION | EXCEPT) all_or_distinct? simple_select_intersect)*
    ;

simple_select_intersect
    : simple_select_pramary (INTERSECT all_or_distinct? simple_select_pramary)*
    ;

simple_select_pramary
    : (
        SELECT
	( all_clause_? target_list_?
		into_clause? from_clause? where_clause?
		group_clause? having_clause? window_clause?
	| distinct_clause target_list
		into_clause? from_clause? where_clause?
		group_clause? having_clause? window_clause?
        )
    )
