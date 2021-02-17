import asyncio

from fox_orm import FoxOrm

FoxOrm.init(DB_URI)


async def main():
    await FoxOrm.connect()
    ...
    await FoxOrm.disconnect()


asyncio.run(main())
