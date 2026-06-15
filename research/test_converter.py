#!/usr/bin/env python3

import pytest

from ms_sql_server_extractor import MsSqlServerExtractor
from converter import convert

NS = "http://schemas.microsoft.com/sqlserver/2004/07/showplan"


def showplan(relop: str) -> str:
    return f"""
    <ShowPlanXML xmlns="{NS}">
      <BatchSequence>
        <Batch>
          <Statements>
            <StmtSimple>
              <QueryPlan>
                {relop}
              </QueryPlan>
            </StmtSimple>
          </Statements>
        </Batch>
      </BatchSequence>
    </ShowPlanXML>
    """


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


def test_aliased_seq_scan_from_xml():
    plan = showplan("""
      <RelOp PhysicalOp="Clustered Index Scan" LogicalOp="Clustered Index Scan">
        <IndexScan>
          <Object Database="[imdb]" Schema="[dbo]" Table="[Titles]" Alias="[t]" Index="[PK_Titles]"/>
        </IndexScan>
      </RelOp>
    """)

    result = convert(plan)

    assert result == "(SeqScan Titles t)"


def test_filter_eq(extractor):
    plan = extract_plan(extractor, "SELECT * FROM Titles WHERE Titles.isAdult = 1")
    result = convert(plan)
    assert result == "(PhysicalFilter (= (attr Titles isAdult) 1) (SeqScan Titles))"


def test_aliased_seq_scan_filter_from_xml():
    plan = showplan("""
      <RelOp PhysicalOp="Clustered Index Scan" LogicalOp="Clustered Index Scan">
        <IndexScan>
          <Object Database="[imdb]" Schema="[dbo]" Table="[Titles]" Alias="[t]" Index="[PK_Titles]"/>
          <Predicate>
            <ScalarOperator>
              <Compare CompareOp="EQ">
                <ScalarOperator>
                  <Identifier>
                    <ColumnReference Database="[imdb]" Schema="[dbo]" Table="[Titles]" Alias="[t]" Column="isAdult"/>
                  </Identifier>
                </ScalarOperator>
                <ScalarOperator>
                  <Const ConstValue="(1)"/>
                </ScalarOperator>
              </Compare>
            </ScalarOperator>
          </Predicate>
        </IndexScan>
      </RelOp>
    """)

    result = convert(plan)

    assert result == "(PhysicalFilter (= (attr t isAdult) 1) (SeqScan Titles t))"


def test_filter_gt(extractor):
    plan = extract_plan(extractor, "SELECT * FROM Titles WHERE Titles.titleId > 5000")
    result = convert(plan)
    assert result == "(IndexSeek (> (attr Titles titleId) 5000) Titles)"


def test_aliased_index_seek_from_xml():
    plan = showplan("""
      <RelOp PhysicalOp="Clustered Index Seek" LogicalOp="Clustered Index Seek">
        <IndexScan>
          <Object Database="[imdb]" Schema="[dbo]" Table="[Titles]" Alias="[t]" Index="[PK_Titles]"/>
          <SeekPredicates>
            <SeekPredicateNew>
              <SeekKeys>
                <StartRange ScanType="GT">
                  <RangeColumns>
                    <ColumnReference Database="[imdb]" Schema="[dbo]" Table="[Titles]" Alias="[t]" Column="titleId"/>
                  </RangeColumns>
                  <RangeExpressions>
                    <ScalarOperator>
                      <Const ConstValue="(5000)"/>
                    </ScalarOperator>
                  </RangeExpressions>
                </StartRange>
              </SeekKeys>
            </SeekPredicateNew>
          </SeekPredicates>
        </IndexScan>
      </RelOp>
    """)

    result = convert(plan)

    assert result == "(IndexSeek (> (attr t titleId) 5000) Titles t)"


def test_index_seek_without_seek_predicates_uses_predicate_from_xml():
    plan = showplan("""
      <RelOp PhysicalOp="Index Seek" LogicalOp="Index Seek">
        <IndexScan>
          <Object Database="[fuzz]" Schema="[dbo]" Table="[customers]" Alias="[t0]" Index="[ix_customers_region_id]"/>
          <Predicate>
            <ScalarOperator>
              <Compare CompareOp="EQ">
                <ScalarOperator>
                  <Identifier>
                    <ColumnReference Database="[fuzz]" Schema="[dbo]" Table="[customers]" Alias="[t0]" Column="region_id"/>
                  </Identifier>
                </ScalarOperator>
                <ScalarOperator>
                  <Const ConstValue="(7)"/>
                </ScalarOperator>
              </Compare>
            </ScalarOperator>
          </Predicate>
        </IndexScan>
      </RelOp>
    """)

    result = convert(plan)

    assert result == "(IndexSeek (= (attr t0 region_id) 7) customers t0)"


def test_index_seek_prefix_eq_from_xml():
    plan = showplan("""
      <RelOp PhysicalOp="Index Seek" LogicalOp="Index Seek">
        <IndexScan>
          <Object Database="[fuzz]" Schema="[dbo]" Table="[customers]" Alias="[t0]" Index="[ix_customers_region_id]"/>
          <SeekPredicates>
            <SeekPredicateNew>
              <SeekKeys>
                <Prefix ScanType="EQ">
                  <RangeColumns>
                    <ColumnReference Database="[fuzz]" Schema="[dbo]" Table="[customers]" Alias="[t0]" Column="region_id"/>
                  </RangeColumns>
                  <RangeExpressions>
                    <ScalarOperator>
                      <Const ConstValue="(7)"/>
                    </ScalarOperator>
                  </RangeExpressions>
                </Prefix>
              </SeekKeys>
            </SeekPredicateNew>
          </SeekPredicates>
        </IndexScan>
      </RelOp>
    """)

    result = convert(plan)

    assert result == "(IndexSeek (= (attr t0 region_id) 7) customers t0)"


def test_index_seek_bounded_range_from_xml():
    plan = showplan("""
      <RelOp PhysicalOp="Index Seek" LogicalOp="Index Seek">
        <IndexScan>
          <Object Database="[fuzz]" Schema="[dbo]" Table="[customers]" Index="[ix_customers_region_id]"/>
          <SeekPredicates>
            <SeekPredicateNew>
              <SeekKeys>
                <StartRange ScanType="GT">
                  <RangeColumns>
                    <ColumnReference Database="[fuzz]" Schema="[dbo]" Table="[customers]" Column="region_id"/>
                  </RangeColumns>
                  <RangeExpressions>
                    <ScalarOperator><Const ConstValue="(3)"/></ScalarOperator>
                  </RangeExpressions>
                </StartRange>
                <EndRange ScanType="LT">
                  <RangeColumns>
                    <ColumnReference Database="[fuzz]" Schema="[dbo]" Table="[customers]" Column="region_id"/>
                  </RangeColumns>
                  <RangeExpressions>
                    <ScalarOperator><Const ConstValue="(8)"/></ScalarOperator>
                  </RangeExpressions>
                </EndRange>
              </SeekKeys>
            </SeekPredicateNew>
          </SeekPredicates>
        </IndexScan>
      </RelOp>
    """)

    result = convert(plan)

    assert result == (
        "(IndexSeek (and (> (attr customers region_id) 3)"
        " (< (attr customers region_id) 8)) customers)"
    )


def test_index_seek_split_ranges_from_xml():
    plan = showplan("""
      <RelOp PhysicalOp="Index Seek" LogicalOp="Index Seek">
        <IndexScan>
          <Object Database="[fuzz]" Schema="[dbo]" Table="[customers]" Index="[ix_customers_region_id]"/>
          <SeekPredicates>
            <SeekPredicateNew>
              <SeekKeys>
                <EndRange ScanType="LT">
                  <RangeColumns>
                    <ColumnReference Database="[fuzz]" Schema="[dbo]" Table="[customers]" Column="region_id"/>
                  </RangeColumns>
                  <RangeExpressions>
                    <ScalarOperator><Const ConstValue="(8)"/></ScalarOperator>
                  </RangeExpressions>
                </EndRange>
              </SeekKeys>
              <SeekKeys>
                <StartRange ScanType="GT">
                  <RangeColumns>
                    <ColumnReference Database="[fuzz]" Schema="[dbo]" Table="[customers]" Column="region_id"/>
                  </RangeColumns>
                  <RangeExpressions>
                    <ScalarOperator><Const ConstValue="(8)"/></ScalarOperator>
                  </RangeExpressions>
                </StartRange>
              </SeekKeys>
            </SeekPredicateNew>
          </SeekPredicates>
        </IndexScan>
      </RelOp>
    """)

    result = convert(plan)

    assert result == (
        "(IndexSeek (or (< (attr customers region_id) 8)"
        " (> (attr customers region_id) 8)) customers)"
    )


def test_nested_loops_outer_reference_index_seek_is_skipped_from_xml():
    plan = showplan("""
      <RelOp PhysicalOp="Nested Loops" LogicalOp="Inner Join">
        <NestedLoops>
          <OuterReferences>
            <ColumnReference Table="[books]" Column="id"/>
          </OuterReferences>
          <RelOp PhysicalOp="Table Scan" LogicalOp="Table Scan">
            <TableScan><Object Table="[books]"/></TableScan>
          </RelOp>
          <RelOp PhysicalOp="Index Seek" LogicalOp="Index Seek">
            <IndexScan>
              <Object Table="[employees]" Index="[ix_employees_id]"/>
              <SeekPredicates>
                <SeekPredicateNew>
                  <SeekKeys>
                    <Prefix ScanType="EQ">
                      <RangeColumns>
                        <ColumnReference Table="[employees]" Column="id"/>
                      </RangeColumns>
                      <RangeExpressions>
                        <ScalarOperator>
                          <Identifier>
                            <ColumnReference Table="[books]" Column="id"/>
                          </Identifier>
                        </ScalarOperator>
                      </RangeExpressions>
                    </Prefix>
                  </SeekKeys>
                </SeekPredicateNew>
              </SeekPredicates>
            </IndexScan>
          </RelOp>
        </NestedLoops>
      </RelOp>
    """)

    with pytest.raises(NotImplementedError, match="outer-reference index seek"):
        convert(plan)


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


# ─── XML-driven tests for the new operators ──────────────────────────────────
# The XML shapes below are inferred from the ShowPlanXML schema; they MUST be
# re-validated against live MS SQL output and captured as ms-server-plans/*.xml
# fixtures (see the plan's Verification step).

def test_string_const_filter_from_xml():
    plan = showplan("""
      <RelOp PhysicalOp="Table Scan" LogicalOp="Table Scan">
        <TableScan>
          <Object Database="[fuzz]" Schema="[dbo]" Table="[markets]" Alias="[m]"/>
          <Predicate>
            <ScalarOperator>
              <Compare CompareOp="EQ">
                <ScalarOperator>
                  <Identifier>
                    <ColumnReference Table="[markets]" Alias="[m]" Column="region"/>
                  </Identifier>
                </ScalarOperator>
                <ScalarOperator>
                  <Const ConstValue="'AMERICA'"/>
                </ScalarOperator>
              </Compare>
            </ScalarOperator>
          </Predicate>
        </TableScan>
      </RelOp>
    """)

    result = convert(plan)

    assert result == '(PhysicalFilter (= (attr m region) (str "AMERICA")) (SeqScan markets m))'


def test_sort_from_xml():
    plan = showplan("""
      <RelOp PhysicalOp="Sort" LogicalOp="Sort">
        <Sort>
          <OrderBy>
            <OrderByColumn Ascending="true">
              <ColumnReference Table="[users]" Column="id"/>
            </OrderByColumn>
            <OrderByColumn Ascending="false">
              <ColumnReference Table="[users]" Column="age"/>
            </OrderByColumn>
          </OrderBy>
          <RelOp PhysicalOp="Table Scan" LogicalOp="Table Scan">
            <TableScan><Object Table="[users]"/></TableScan>
          </RelOp>
        </Sort>
      </RelOp>
    """)

    result = convert(plan)

    assert result == "(Sort (keys users.id Asc users.age Desc) (SeqScan users))"


def test_stream_aggregate_drops_enforcer_sort_from_xml():
    plan = showplan("""
      <RelOp PhysicalOp="Stream Aggregate" LogicalOp="Aggregate">
        <StreamAggregate>
          <DefinedValues>
            <DefinedValue>
              <ColumnReference Column="Expr1001"/>
              <ScalarOperator>
                <Aggregate AggType="SUM" Distinct="false">
                  <ScalarOperator>
                    <Identifier><ColumnReference Table="[users]" Column="age"/></Identifier>
                  </ScalarOperator>
                </Aggregate>
              </ScalarOperator>
            </DefinedValue>
            <DefinedValue>
              <ColumnReference Column="Expr1002"/>
              <ScalarOperator>
                <Aggregate AggType="countstar" Distinct="false"/>
              </ScalarOperator>
            </DefinedValue>
          </DefinedValues>
          <GroupBy>
            <ColumnReference Table="[users]" Column="id"/>
          </GroupBy>
          <RelOp PhysicalOp="Sort" LogicalOp="Aggregate">
            <Sort>
              <OrderBy>
                <OrderByColumn Ascending="true">
                  <ColumnReference Table="[users]" Column="id"/>
                </OrderByColumn>
              </OrderBy>
              <RelOp PhysicalOp="Table Scan" LogicalOp="Table Scan">
                <TableScan><Object Table="[users]"/></TableScan>
              </RelOp>
            </Sort>
          </RelOp>
        </StreamAggregate>
      </RelOp>
    """)

    result = convert(plan)

    assert result == (
        "(HashAggregate (group_by (attr users id))"
        " (aggs (SUM (attr users age)) (COUNT *)) (SeqScan users))"
    )


def test_hash_aggregate_from_xml():
    plan = showplan("""
      <RelOp PhysicalOp="Hash Match" LogicalOp="Aggregate">
        <Hash>
          <DefinedValues>
            <DefinedValue>
              <ColumnReference Column="Expr1001"/>
              <ScalarOperator>
                <Aggregate AggType="COUNT" Distinct="false">
                  <ScalarOperator>
                    <Identifier><ColumnReference Table="[users]" Column="age"/></Identifier>
                  </ScalarOperator>
                </Aggregate>
              </ScalarOperator>
            </DefinedValue>
          </DefinedValues>
          <HashKeysBuild>
            <ColumnReference Table="[users]" Column="id"/>
          </HashKeysBuild>
          <RelOp PhysicalOp="Table Scan" LogicalOp="Table Scan">
            <TableScan><Object Table="[users]"/></TableScan>
          </RelOp>
        </Hash>
      </RelOp>
    """)

    result = convert(plan)

    assert result == (
        "(HashAggregate (group_by (attr users id))"
        " (aggs (COUNT (attr users age))) (SeqScan users))"
    )


def test_compute_scalar_passthrough_from_xml():
    plan = showplan("""
      <RelOp PhysicalOp="Compute Scalar" LogicalOp="Compute Scalar">
        <ComputeScalar>
          <RelOp PhysicalOp="Table Scan" LogicalOp="Table Scan">
            <TableScan><Object Table="[users]"/></TableScan>
          </RelOp>
        </ComputeScalar>
      </RelOp>
    """)

    result = convert(plan)

    assert result == "(SeqScan users)"
