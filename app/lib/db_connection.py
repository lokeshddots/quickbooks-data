import pdb
import sqlalchemy
from sqlalchemy.engine.mock import MockConnection

import config

# pdb.set_trace()
db_engine: MockConnection = sqlalchemy.create_engine("mysql+mysqldb://root@localhost/link_unfurling", pool_pre_ping=True)