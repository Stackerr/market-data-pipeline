import os
from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

Base = declarative_base()

logger = logging.getLogger(__name__)


class DatabaseConnection:
    def __init__(self):
        self.engine: Engine = None
        self.SessionLocal: sessionmaker = None
        self._setup_database()

    def _setup_database(self):
        """Setup database connection and session factory"""
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = os.getenv("DB_PORT", "5432")
        db_name = os.getenv("DB_NAME", "market_data")
        db_user = os.getenv("DB_USER", "postgres")
        db_password = os.getenv("DB_PASSWORD", "")

        database_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
        self.engine = create_engine(
            database_url,
            pool_size=int(os.getenv("DB_POOL_SIZE", "10")),
            max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "20")),
            echo=False  # Set to True for SQL debugging
        )
        
        self.SessionLocal = sessionmaker(
            autocommit=False, 
            autoflush=False, 
            bind=self.engine
        )
        
        logger.info(f"Database connection established to {db_host}:{db_port}/{db_name}")

    def get_session(self) -> Session:
        """Get a database session"""
        return self.SessionLocal()

    def create_tables(self):
        """Create all tables defined in models"""
        Base.metadata.create_all(bind=self.engine)
        logger.info("Database tables created successfully")

    def drop_tables(self):
        """Drop all tables (use with caution)"""
        Base.metadata.drop_all(bind=self.engine)
        logger.warning("All database tables dropped")


# Global database connection instance
db_connection = DatabaseConnection()

def get_db_session() -> Session:
    """Dependency for getting database session"""
    session = db_connection.get_session()
    try:
        yield session
    finally:
        session.close()