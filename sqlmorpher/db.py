from sqlalchemy import create_engine, Engine, MetaData, text
from sqlalchemy.engine import RowMapping
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from pydantic import validate_call
from typing import Optional, Any, Dict, Sequence


class Database:
    type: str
    engine: Engine
    metadata: MetaData
    session_factory: sessionmaker[Session]

    @validate_call
    def __init__(self, type: str, connection_string: str):
        self.type = type
        self.engine = create_engine(connection_string)
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)
        self.session_factory = sessionmaker(bind=self.engine)

    @validate_call
    def execute_query(
        self, query: str, params: Optional[dict[str, Any]] = None
    ) -> Optional[Sequence[RowMapping]]:
        session: Session = self.session_factory()
        try:
            result: Any = session.execute(text(query), params)
            session.commit()

            if getattr(result, "returns_rows", False):
                return result.mappings().fetchall()
            return None
        except SQLAlchemyError as e:
            session.rollback()
            raise e
        finally:
            session.close()

    @validate_call
    def insert_row(self, table: str, row: Dict[str, Any]) -> None:
        columns_list = list(row.keys())
        placeholders = ", ".join([f":{c}" for c in columns_list])
        columns_str = ", ".join(columns_list)
        insert_sql = (
            f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
        )
        self.execute_query(insert_sql, params=row)

    def reflect_tables(self) -> None:
        self.metadata.reflect(bind=self.engine)

    def validate_connection(self) -> bool:
        try:
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            return True
        except SQLAlchemyError:
            return False
