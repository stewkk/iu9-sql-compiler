#!/usr/bin/env python3

import pytest

from ms_sql_server_extractor import MsSqlServerExtractor
from converter import convert


@pytest.fixture(scope="session")
def extractor():
    return MsSqlServerExtractor(
        dataset="./datasets",
        host="localhost",
        port=1433,
        user="sa",
        password="Password123!",
    )


def extract_plan(extractor, sql: str) -> str:
    """Extract plan with OPTION (RECOMPILE) so literal values appear in the XML instead of @N placeholders."""
    return extractor.extract(sql + " OPTION (RECOMPILE)")


def test_seq_scan(extractor):
    plan = extract_plan(extractor, "SELECT * FROM Titles")
    result = convert(plan)
    assert result == "(SeqScan Titles)"


def test_filter_eq(extractor):
    plan = extract_plan(extractor, "SELECT * FROM Titles WHERE Titles.isAdult = 1")
    result = convert(plan)
    assert result == "(PhysicalFilter (= (attr Titles isAdult) 1) (SeqScan Titles))"


def test_filter_gt(extractor):
    plan = extract_plan(extractor, "SELECT * FROM Titles WHERE Titles.titleId > 5000")
    result = convert(plan)
    assert result == "(IndexSeek (> (attr Titles titleId) 5000) Titles)"


def test_filter_lt(extractor):
    plan = extract_plan(extractor, "SELECT * FROM Titles WHERE Titles.titleId < 5000")
    result = convert(plan)
    assert result == "(IndexSeek (< (attr Titles titleId) 5000) Titles)"


def test_filter_and(extractor):
    plan = extract_plan(extractor, "SELECT * FROM Titles WHERE Titles.isAdult = 1 AND Titles.titleId > 5000")
    result = convert(plan)
    assert result == "(PhysicalFilter (= (attr Titles isAdult) 1) (IndexSeek (> (attr Titles titleId) 5000) Titles))"


@pytest.mark.skip(reason="IS NULL not supported in target plan format (BinaryOp lacks kIsNull)")
def test_filter_or(extractor):
    plan = extract_plan(extractor, "SELECT * FROM Titles WHERE Titles.isAdult = 0 OR Titles.endYear IS NULL")
    result = convert(plan)
    assert result == ""


def test_filter_not(extractor):
    plan = extract_plan(extractor, "SELECT * FROM Titles WHERE NOT Titles.isAdult = 1")
    result = convert(plan)
    assert result == "(PhysicalFilter (!= (attr Titles isAdult) 1) (SeqScan Titles))"


def test_cross_join(extractor):
    plan = extract_plan(extractor, "SELECT * FROM TitleTypes CROSS JOIN Genres")
    result = convert(plan)
    assert result == "(NestedLoopCrossJoin (SeqScan TitleTypes) (SeqScan Genres))"


def test_inner_join(extractor):
    plan = extract_plan(extractor,
        "SELECT * FROM TitlePrincipals JOIN Principals ON TitlePrincipals.principalId = Principals.principalId"
    )
    result = convert(plan)
    assert result == "(HashJoin Inner (= (attr Principals principalId) (attr TitlePrincipals principalId)) (SeqScan Principals) (SeqScan TitlePrincipals))"


def test_left_join(extractor):
    plan = extract_plan(extractor,
        "SELECT * FROM Titles LEFT OUTER JOIN Episodes ON Titles.titleId = Episodes.episodeId"
    )
    result = convert(plan)
    assert result == "(MergeJoin Left (= (attr Titles titleId) (attr Episodes episodeId)) (SeqScan Titles) (SeqScan Episodes))"


def test_join_with_filter(extractor):
    plan = extract_plan(extractor,
        "SELECT * FROM TitlePrincipals JOIN Principals ON TitlePrincipals.principalId = Principals.principalId WHERE TitlePrincipals.ordinal > 1"
    )
    result = convert(plan)
    assert result == "(HashJoin Inner (= (attr Principals principalId) (attr TitlePrincipals principalId)) (SeqScan Principals) (PhysicalFilter (> (attr TitlePrincipals ordinal) 1) (SeqScan TitlePrincipals)))"


def test_join_compound_predicate(extractor):
    plan = extract_plan(extractor,
        "SELECT * FROM TitlePrincipals JOIN Principals ON TitlePrincipals.principalId = Principals.principalId AND TitlePrincipals.ordinal = 1"
    )
    result = convert(plan)
    assert result == "(HashJoin Inner (= (attr TitlePrincipals principalId) (attr Principals principalId)) (PhysicalFilter (= (attr TitlePrincipals ordinal) 1) (SeqScan TitlePrincipals)) (SeqScan Principals))"
