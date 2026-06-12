#!/usr/bin/env python3
"""Phase 0 helper: pretty-print an MS SQL Server execution plan XML."""

import sys
import xml.dom.minidom

from ms_sql_server_extractor import MsSqlServerExtractor

QUERY = sys.argv[1] if len(sys.argv) > 1 else "SELECT * FROM dbo.Titles WHERE isAdult = 1"

extractor = MsSqlServerExtractor(
    dataset="./datasets",
    host="localhost",
    port=1433,
    user="sa",
    password="Password123!",
)

xml_str = extractor.extract(QUERY)
print(xml.dom.minidom.parseString(xml_str).toprettyxml(indent="  "))
