from databases import Database
from sqlalchemy import MetaData


class Connection:
    _db: Database | None
    _metadata: MetaData
    db: Database | None
    metadata: MetaData

    def __init__(self):
        self._db = None
        self._metadata = MetaData()

    def init(self, db_url: str, **db_options):
        self._db = Database(db_url, **db_options)

    @property
    def db(self) -> Database:
        return self._db

    @property
    def metadata(self) -> MetaData:
        return self._metadata

    async def connect(self):
        await self.db.connect()

    async def disconnect(self):
        await self.db.disconnect()


__all__ = ['Connection']
