"""
Create ClickHouse master stock information table
"""

from clickhouse_connect import get_client
import logging
import sys
import os

# Add parent directory to path to enable imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import get_clickhouse_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_master_stock_table():
    """Create master stock information table in ClickHouse"""

    client = get_clickhouse_client()

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS stock_master (
        stock_code String,
        stock_name String,
        listing_date Date,
        delisting_date Nullable(Date),
        update_dt DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY stock_code
    SETTINGS index_granularity = 8192
    """

    try:
        client.command(create_table_sql)
        logger.info("Master stock table created successfully")

        # Create index for better query performance
        index_sql = """
        CREATE INDEX IF NOT EXISTS idx_stock_code ON stock_master (stock_code) TYPE bloom_filter GRANULARITY 1
        """
        client.command(index_sql)
        logger.info("Index created successfully")

    except Exception as e:
        logger.error(f"Error creating master stock table: {e}")
        raise
    finally:
        client.close()


if __name__ == "__main__":
    create_master_stock_table()