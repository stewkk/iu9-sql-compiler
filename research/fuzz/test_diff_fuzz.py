from research.fuzz.diff_fuzz import _check_order
from research.query_generator import Attr, SelectQuery, TableScan, Target


def _query() -> SelectQuery:
    users_id = Attr("users", "id")
    regions_id = Attr("regions", "id")
    return SelectQuery(
        targets=[Target(users_id), Target(regions_id)],
        from_=TableScan("users"),
        order_by=[(users_id, "desc"), (regions_id, "desc")],
    )


def test_check_order_does_not_use_later_key_after_null_difference() -> None:
    assert _check_order(_query(), ["10\t10", "1\t1", "NULL\t9"]) is None


def test_check_order_uses_later_key_when_earlier_null_keys_are_equal() -> None:
    assert _check_order(_query(), ["NULL\t1", "NULL\t9"]) is not None


def test_check_order_rejects_non_null_ordering_violation() -> None:
    assert _check_order(_query(), ["10\t10", "1\t1", "5\t5"]) is not None
