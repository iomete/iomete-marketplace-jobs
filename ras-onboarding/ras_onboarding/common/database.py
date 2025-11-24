"""Database connection and transaction management."""

import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from typing import Dict, Any, List, Optional
from .logger import get_logger

logger = get_logger(__name__)


class DatabaseManager:
    """Manages database connections and transactions."""

    def __init__(self, db_config: Dict[str, Any], debug_mode: bool = False):
        """
        Initialize database manager.

        Args:
            db_config: Database configuration dictionary
            debug_mode: Enable debug logging for queries
        """
        self.db_config = db_config
        self.debug_mode = debug_mode
        self.connection = None

    def get_connection_string(self) -> str:
        """Get PostgreSQL connection string."""
        return (
            f"host={self.db_config['host']} "
            f"port={self.db_config['port']} "
            f"dbname={self.db_config['name']} "
            f"user={self.db_config['user']} "
            f"password={self.db_config['password']} "
            f"sslmode={self.db_config.get('ssl_mode', 'require')}"
        )

    @contextmanager
    def get_connection(self):
        """Get database connection with automatic cleanup."""
        connection = None
        try:
            connection = psycopg2.connect(self.get_connection_string())
            yield connection
        except Exception as e:
            if connection:
                connection.rollback()
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if connection:
                connection.close()

    @contextmanager
    def get_transaction(self):
        """Get database transaction with automatic commit/rollback."""
        with self.get_connection() as connection:
            try:
                yield connection
                connection.commit()
                logger.info("Transaction committed successfully")
            except Exception as e:
                connection.rollback()
                logger.error(f"Transaction rolled back due to error: {e}")
                raise

    def execute_query(self, connection, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute a query and return results.

        Args:
            connection: Database connection
            query: SQL query to execute
            params: Query parameters

        Returns:
            List of dictionaries representing query results
        """
        if self.debug_mode:
            logger.debug(f"Executing query: {query}")
            if params:
                logger.debug(f"Query parameters: {params}")

        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(query, params)
            if cursor.description:
                results = [dict(row) for row in cursor.fetchall()]
                if self.debug_mode:
                    logger.debug(f"Query returned {len(results)} rows")
                return results
            return []

    def execute_insert(self, connection, query: str, params: Optional[tuple] = None) -> str:
        """
        Execute an insert query and return the generated ID.

        Args:
            connection: Database connection
            query: SQL insert query
            params: Query parameters

        Returns:
            Generated UUID from the insert
        """
        if self.debug_mode:
            logger.debug(f"Executing insert query: {query}")
            if params:
                logger.debug(f"Insert parameters: {params}")

        with connection.cursor() as cursor:
            cursor.execute(query, params)
            if cursor.description and cursor.rowcount > 0:
                result = cursor.fetchone()
                generated_id = result[0] if result else None
                if self.debug_mode:
                    logger.debug(f"Insert generated ID: {generated_id}")
                return generated_id
            return None

    def test_connection(self) -> bool:
        """Test database connectivity."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    return result[0] == 1
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False