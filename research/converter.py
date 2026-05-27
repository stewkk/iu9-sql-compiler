#!/usr/bin/env python3
"""
Converter from MS SQL Server ShowPlanXML to serialized physical plan format.

Serialized format (s-expressions):
  (SeqScan table)
  (PhysicalFilter EXPR SOURCE)
  (PhysicalProjection (exprs EXPR...) SOURCE)
  (NestedLoopCrossJoin LHS RHS)
  (NestedLoopJoin JoinType EXPR LHS RHS)   JoinType: Inner | Full | Left | Right

Expressions:
  42  NULL  TRUE  FALSE  UNKNOWN
  (attr table column)
  (OP LHS RHS)   OP: = != > < >= <= and or + - * / % ^
  (not EXPR)
  (uminus EXPR)
  (isnull EXPR)
"""

import xml.etree.ElementTree as ET

NS = "{http://schemas.microsoft.com/sqlserver/2004/07/showplan}"

_JOIN_TYPES = {
    "Inner Join":      "Inner",
    "Left Outer Join": "Left",
    "Right Outer Join":"Right",
    "Full Outer Join": "Full",
}

_COMPARE_OPS = {
    "EQ": "=",
    "NE": "!=",
    "GT": ">",
    "LT": "<",
    "GE": ">=",
    "LE": "<=",
}

_LOGICAL_OPS = {
    "AND": "and",
    "OR":  "or",
}

_ARITHMETIC_OPS = {
    "ADD": "+",
    "SUB": "-",
    "MUL": "*",
    "DIV": "/",
    "MOD": "%",
}


def convert(plan_xml: str) -> str:
    root = ET.fromstring(plan_xml)
    top_relop = root.find(f".//{NS}RelOp")
    if top_relop is None:
        raise ValueError("no RelOp found in plan XML")
    return _convert_relop(top_relop)


# ─── Operator conversion ─────────────────────────────────────────────────────

def _convert_relop(relop: ET.Element) -> str:
    phys_op = relop.get("PhysicalOp", "")
    logical_op = relop.get("LogicalOp", "")

    if phys_op in ("Clustered Index Scan", "Index Scan", "Table Scan",
                   "Clustered Index Seek", "Index Seek"):
        return _convert_scan(relop)
    if phys_op == "Nested Loops":
        return _convert_nested_loops(relop, logical_op)
    if phys_op == "Hash Match":
        return _convert_hash_match(relop, logical_op)
    if phys_op == "Merge Join":
        return _convert_merge_join(relop, logical_op)

    raise NotImplementedError(f"unhandled PhysicalOp: {phys_op!r} (LogicalOp={logical_op!r})")


_SEEK_SCAN_TYPES = {"GT": ">", "GE": ">=", "LT": "<", "LE": "<=", "EQ": "="}


def _convert_scan(relop: ET.Element) -> str:
    phys_op = relop.get("PhysicalOp", "")
    obj = relop.find(f".//{NS}IndexScan/{NS}Object")
    if obj is None:
        obj = relop.find(f".//{NS}TableScan/{NS}Object")
    if obj is None:
        raise ValueError("cannot find Object element in scan")
    table = (obj.get("Alias") or obj.get("Table", "")).strip("[]")

    if "Seek" in phys_op:
        seek_pred = _convert_seek_predicates(relop, table)
        if seek_pred is None:
            raise ValueError(f"Index Seek node has no SeekPredicates: {phys_op!r}")
        base = f"(IndexSeek {seek_pred} {table})"
    else:
        base = f"(SeqScan {table})"

    # Residual predicate (pushed-down filter evaluated after the scan/seek)
    pred_elem = relop.find(f".//{NS}IndexScan/{NS}Predicate/{NS}ScalarOperator")
    if pred_elem is None:
        pred_elem = relop.find(f".//{NS}TableScan/{NS}Predicate/{NS}ScalarOperator")
    if pred_elem is not None:
        return f"(PhysicalFilter {_convert_scalar(pred_elem)} {base})"

    return base


def _convert_seek_predicates(relop: ET.Element, table: str) -> str | None:
    seek_preds = relop.find(f".//{NS}SeekPredicates")
    if seek_preds is None:
        return None

    conditions = []
    for range_elem in (seek_preds.findall(f".//{NS}StartRange") +
                       seek_preds.findall(f".//{NS}EndRange")):
        op = _SEEK_SCAN_TYPES.get(range_elem.get("ScanType", ""))
        if not op:
            continue
        col_refs = range_elem.findall(f"{NS}RangeColumns/{NS}ColumnReference")
        expr_elems = range_elem.findall(f"{NS}RangeExpressions/{NS}ScalarOperator")
        for col_ref, expr_elem in zip(col_refs, expr_elems):
            col = col_ref.get("Column", "").strip("[]")
            col_table = (col_ref.get("Alias") or col_ref.get("Table") or table).strip("[]")
            conditions.append(f"({op} (attr {col_table} {col}) {_convert_scalar(expr_elem)})")

    if not conditions:
        return None
    result = conditions[0]
    for c in conditions[1:]:
        result = f"(and {result} {c})"
    return result


def _convert_nested_loops(relop: ET.Element, logical_op: str) -> str:
    nl = relop.find(f"{NS}NestedLoops")
    if nl is None:
        raise ValueError("NestedLoops element missing")
    lhs, rhs = _expect_two_relops(nl, "NestedLoops")

    pred_elem = nl.find(f"{NS}Predicate/{NS}ScalarOperator")
    if pred_elem is None:
        return f"(NestedLoopCrossJoin {_convert_relop(lhs)} {_convert_relop(rhs)})"

    join_type = _JOIN_TYPES.get(logical_op, "Inner")
    pred = _convert_scalar(pred_elem)
    return f"(NestedLoopJoin {join_type} {pred} {_convert_relop(lhs)} {_convert_relop(rhs)})"


def _convert_hash_match(relop: ET.Element, logical_op: str) -> str:
    hash_elem = relop.find(f"{NS}Hash")
    if hash_elem is None:
        raise ValueError("Hash element missing from Hash Match node")
    build_relop, probe_relop = _expect_two_relops(hash_elem, "Hash")

    build_refs = hash_elem.findall(f"{NS}HashKeysBuild/{NS}ColumnReference")
    probe_refs = hash_elem.findall(f"{NS}HashKeysProbe/{NS}ColumnReference")
    if not build_refs or len(build_refs) != len(probe_refs):
        raise ValueError("cannot reconstruct Hash Match predicate: key lists empty or mismatched")

    pairs = [f"(= {_col_ref_to_attr(b)} {_col_ref_to_attr(p)})"
             for b, p in zip(build_refs, probe_refs)]
    pred = pairs[0]
    for p in pairs[1:]:
        pred = f"(and {pred} {p})"

    join_type = _JOIN_TYPES.get(logical_op, "Inner")
    return f"(HashJoin {join_type} {pred} {_convert_relop(build_relop)} {_convert_relop(probe_relop)})"


def _convert_merge_join(relop: ET.Element, logical_op: str) -> str:
    merge = relop.find(f"{NS}Merge")
    if merge is None:
        raise ValueError("Merge element missing from Merge Join node")
    outer_relop, inner_relop = _expect_two_relops(merge, "Merge")

    pred_elem = merge.find(f"{NS}Residual/{NS}ScalarOperator")
    if pred_elem is None:
        raise NotImplementedError("Merge Join with no Residual predicate")

    join_type = _JOIN_TYPES.get(logical_op, "Inner")
    pred = _convert_scalar(pred_elem)
    return f"(MergeJoin {join_type} {pred} {_convert_relop(outer_relop)} {_convert_relop(inner_relop)})"


def _expect_two_relops(parent: ET.Element, label: str) -> tuple[ET.Element, ET.Element]:
    children = parent.findall(f"{NS}RelOp")
    if len(children) != 2:
        raise ValueError(f"{label} has {len(children)} RelOp children, expected 2")
    return children[0], children[1]


def _col_ref_to_attr(cr: ET.Element) -> str:
    table = (cr.get("Alias") or cr.get("Table", "")).strip("[]")
    col = cr.get("Column", "").strip("[]")
    return f"(attr {table} {col})"


# ─── Scalar expression conversion ────────────────────────────────────────────

def _convert_scalar(scalar: ET.Element) -> str:
    convert = scalar.find(f"{NS}Convert")
    if convert is not None:
        child = convert.find(f"{NS}ScalarOperator")
        if child is None:
            raise ValueError("Convert has no child ScalarOperator")
        return _convert_scalar(child)

    compare = scalar.find(f"{NS}Compare")
    if compare is not None:
        return _convert_compare(compare)

    logical = scalar.find(f"{NS}Logical")
    if logical is not None:
        return _convert_logical(logical)

    arithmetic = scalar.find(f"{NS}Arithmetic")
    if arithmetic is not None:
        return _convert_arithmetic(arithmetic)

    identifier = scalar.find(f"{NS}Identifier")
    if identifier is not None:
        return _convert_identifier(identifier)

    const = scalar.find(f"{NS}Const")
    if const is not None:
        return _convert_const(const)

    raise NotImplementedError(f"unhandled ScalarOperator children: {[c.tag.split('}')[1] for c in scalar]}")


def _convert_compare(compare: ET.Element) -> str:
    op_str = compare.get("CompareOp", "")
    scalars = compare.findall(f"{NS}ScalarOperator")

    if op_str in ("IS", "IS NOT"):
        # MS SQL emits "lhs IS NULL" / "lhs IS NOT NULL" as a Compare where the
        # rhs is a NULL constant. Map to the project's (isnull lhs) / (not (isnull lhs)).
        if len(scalars) != 2:
            raise ValueError(f"IS/IS NOT Compare has {len(scalars)} ScalarOperators, expected 2")
        lhs = _convert_scalar(scalars[0])
        rhs_const = scalars[1].find(f"{NS}Const")
        if rhs_const is None or rhs_const.get("ConstValue", "").upper() not in ("NULL", "(NULL)"):
            raise NotImplementedError(
                f"CompareOp={op_str!r} with non-NULL rhs (IS DISTINCT FROM-like) is not supported"
            )
        isnull = f"(isnull {lhs})"
        return f"(not {isnull})" if op_str == "IS NOT" else isnull

    op = _COMPARE_OPS.get(op_str)
    if op is None:
        raise NotImplementedError(f"unhandled CompareOp: {op_str!r}")

    if len(scalars) != 2:
        raise ValueError(f"Compare has {len(scalars)} ScalarOperator children, expected 2")
    return f"({op} {_convert_scalar(scalars[0])} {_convert_scalar(scalars[1])})"


def _convert_logical(logical: ET.Element) -> str:
    op_str = logical.get("Operation", "").upper()
    children = [_convert_scalar(s) for s in logical.findall(f"{NS}ScalarOperator")]

    if op_str == "NOT":
        if len(children) != 1:
            raise ValueError(f"NOT expects 1 child, got {len(children)}")
        return f"(not {children[0]})"

    op = _LOGICAL_OPS.get(op_str)
    if op is None:
        raise NotImplementedError(f"unhandled Logical Operation: {op_str!r}")
    # MS SQL AND/OR can be n-ary — fold left into binary pairs.
    result = children[0]
    for child in children[1:]:
        result = f"({op} {result} {child})"
    return result


def _convert_arithmetic(arithmetic: ET.Element) -> str:
    op_str = arithmetic.get("Operation", "").upper()
    op = _ARITHMETIC_OPS.get(op_str)
    if op is None:
        raise NotImplementedError(f"unhandled Arithmetic Operation: {op_str!r}")
    scalars = arithmetic.findall(f"{NS}ScalarOperator")
    if len(scalars) != 2:
        raise ValueError(f"Arithmetic has {len(scalars)} ScalarOperator children, expected 2")
    return f"({op} {_convert_scalar(scalars[0])} {_convert_scalar(scalars[1])})"


def _convert_identifier(identifier: ET.Element) -> str:
    col_ref = identifier.find(f"{NS}ColumnReference")
    if col_ref is None:
        raise ValueError("Identifier has no ColumnReference")
    column = col_ref.get("Column", "").strip("[]")

    if column.startswith("ConstExpr"):
        child = col_ref.find(f"{NS}ScalarOperator")
        if child is None:
            raise ValueError(f"ConstExpr {column!r} has no child ScalarOperator")
        return _convert_scalar(child)

    if column.startswith("@"):
        raise NotImplementedError(
            f"parameter reference {column!r} — use OPTION (RECOMPILE) to embed literal values in the plan"
        )

    table = (col_ref.get("Alias") or col_ref.get("Table", "")).strip("[]")
    return f"(attr {table} {column})"


def _convert_const(const: ET.Element) -> str:
    value = const.get("ConstValue", "").strip("()")
    return value
