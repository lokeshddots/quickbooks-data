import sqlalchemy
from sqlalchemy.engine.mock import MockConnection

import config

db_engine: MockConnection = sqlalchemy.create_engine(url=config.DATABASE_URL)
