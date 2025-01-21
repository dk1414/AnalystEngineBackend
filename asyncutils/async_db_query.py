import json
import csv
from io import StringIO
import sqlalchemy
from sqlalchemy.ext.asyncio import AsyncEngine

async def execute_sql_query_async(
    engine: AsyncEngine,
    query: str,
    output_format: str = "csv"
) -> str:
    """
    Executes a SQL query using an async SQLAlchemy engine,
    returns data as JSON or CSV string.
    """
    if output_format not in ("json", "csv"):
        raise ValueError("Invalid output_format. Must be 'json' or 'csv'.")

    async with engine.connect() as conn:
        # Execute query
        result = await conn.execute(sqlalchemy.text(query))

        # 'result.mappings()' returns RowMapping objects
        rows = result.mappings().all()

        if not rows:
            colnames = []
        else:
            colnames = list(rows[0].keys())

        if output_format == "json":
            # Convert RowMapping -> dict
            # Then dump to JSON
            data_dicts = [dict(r) for r in rows]
            return json.dumps(data_dicts, default=str)
        else:
            # CSV
            output = StringIO()
            writer = csv.DictWriter(output, fieldnames=colnames, quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            for row_map in rows:
                writer.writerow(dict(row_map))
            return output.getvalue()
