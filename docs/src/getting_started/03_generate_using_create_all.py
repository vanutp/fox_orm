from sqlalchemy import create_engine

from fox_orm import FoxOrm

engine = create_engine(DB_URI)
FoxOrm.metadata.create_all(engine)
