import json
import csv
from io import StringIO
import sqlalchemy

def execute_sql_query(conn, query: str, output_format: str = "json") -> str:
    """
    Executes a SQL query using a given SQLAlchemy connection 'conn'.
    Returns the result as either a JSON string or CSV string, depending on 'output_format'.
    """

    if output_format not in ("json", "csv"):
        raise ValueError("Invalid output_format. Must be 'json' or 'csv'.")

    try:
        result = conn.execute(sqlalchemy.text(query))
        # Use 'mappings()' to get each row as a dictionary
        rows = result.mappings().all()  # a list of RowMapping objects

        # Convert RowMapping objects to plain dictionaries (optional)
        # rows = [dict(r) for r in result.mappings().all()]

        if not rows:
            colnames = []
        else:
            # If we kept them as row mappings, we can do:
            colnames = list(rows[0].keys())

        # Return data as JSON
        if output_format == "json":
            return json.dumps(rows, default=str)

        # Or return data as CSV
        else:
            output = StringIO()
            writer = csv.DictWriter(output, fieldnames=colnames, quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            for row_map in rows:
                writer.writerow(row_map)
            return output.getvalue()

    except Exception as e:
        raise RuntimeError(f"Error executing query: {e}") from e

