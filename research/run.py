import extractor, ms_sql_server_extractor

from ms_sql_server_extractor import MsSqlServerExtractor

extractor_obj = MsSqlServerExtractor(
    dataset="/home/st/c/iu9-sql-compiler/research/datasets/mysql-data/imdb",
    host="localhost",
    port=1433,
    user="sa",
    password="Password123!",
    max_rows=10_000,
)

query = """
SELECT p.primaryName, t.averageRating
FROM dbo.TitlePrincipals tp
JOIN dbo.Principals p ON p.principalId = tp.principalId
JOIN dbo.Titles t ON t.titleId = tp.titleId
WHERE t.averageRating IS NOT NULL
"""

plan = extractor_obj.extract(query)
print(plan)