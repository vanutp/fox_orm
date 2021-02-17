from sqlalchemy import create_engine

from models import metadata

engine = create_engine(DB_URI)
metadata.create_all(engine)
