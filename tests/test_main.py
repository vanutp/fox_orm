import unittest
from datetime import datetime, date, time, timedelta
from time import sleep
from typing import List, Optional, Dict

import sqlalchemy as sa
import sqlalchemy.engine
from fox_orm import FoxOrm, Connection, OrmModel
from fox_orm.column import jsonb
from fox_orm.column.flags import pkey, fkey, null, autoincrement, index, unique
from fox_orm.exceptions import *
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
from tests.models import User, Group, Message
from tests.utils import (
    schema_to_set,
    port_occupied,
    start_postgres,
    stop_container,
    try_connect_postgres,
)


class TestMain(unittest.IsolatedAsyncioTestCase):
    engine: sqlalchemy.engine.Engine
    postgres_container_id: str

    @classmethod
    def setUpClass(cls):
        port = 5433
        while port_occupied(port):
            port += 1
        cls.postgres_container_id = start_postgres(port)
        try:
            db_uri = f'postgresql://postgres:postgres@localhost:{port}/postgres'
            sleep(3)
            while not try_connect_postgres(db_uri):
                sleep(0.01)
            FoxOrm.init(db_uri)
            cls.engine = create_engine(db_uri)
            FoxOrm.metadata.create_all(cls.engine)
        except:
            stop_container(cls.postgres_container_id)
            raise

    async def asyncSetUp(self):
        await FoxOrm.connect()

    async def asyncTearDown(self):
        await FoxOrm.disconnect()

    @classmethod
    def tearDownClass(cls):
        stop_container(cls.postgres_container_id)

    async def test_table_generation(self):
        class AllTypesHelper(BaseModel):
            a: str
            b: int

        class AllTypesRelHelper(OrmModel):
            pkey: int | None = pkey

        class AllTypes(OrmModel):
            pkey: int = pkey
            int_: int = null
            float_: float = ~null
            str_: Optional[str]
            bool_: bool = autoincrement

            datetime_: datetime = index
            date_: date = unique
            time_: time
            timedelta_: timedelta

            dict_: dict | None
            list_: list
            typed_dict: Dict[str, str]
            typed_dict_2: dict[str, str]
            typed_list: List[int]
            typed_list_2: list[int] = jsonb
            implicit_json: AllTypesHelper

            foreign_key: int = fkey('all_types_rel_helper.pkey')
            foreign_key_2: int = fkey(AllTypesRelHelper.pkey)

        all_types_proper_schema = {
            ('pkey', sa.Integer),
            ('int_', sa.Integer),
            ('float_', sa.Float),
            ('str_', sa.String),
            ('bool_', sa.Boolean),
            ('datetime_', sa.DateTime),
            ('date_', sa.Date),
            ('time_', sa.Time),
            ('timedelta_', sa.Interval),
            ('dict_', sa.JSON),
            ('list_', sa.JSON),
            ('typed_dict', sa.JSON),
            ('typed_dict_2', sa.JSON),
            ('typed_list', sa.JSON),
            ('typed_list_2', JSONB),
            ('implicit_json', sa.JSON),
            ('foreign_key', sa.Integer),
            ('foreign_key_2', sa.Integer),
        }
        self.assertEqual(schema_to_set(AllTypes.__table__), all_types_proper_schema)
        self.assertFalse(AllTypes.pkey.nullable)
        self.assertTrue(AllTypes.int_.nullable)
        self.assertFalse(AllTypes.float_.nullable)
        self.assertTrue(AllTypes.str_.nullable)
        self.assertTrue(AllTypes.bool_.autoincrement)
        self.assertTrue(AllTypes.dict_.nullable)
        self.assertTrue(AllTypes.datetime_.index)
        self.assertTrue(AllTypes.date_.unique)
        self.assertEqual(
            list(AllTypes.__table__.c.foreign_key.foreign_keys)[0].column,
            AllTypesRelHelper.pkey,
        )
        self.assertEqual(
            list(AllTypes.__table__.c.foreign_key_2.foreign_keys)[0].column,
            AllTypesRelHelper.pkey,
        )

    async def test_relationship_generation(self):
        from sqlalchemy import MetaData, Integer
        from fox_orm import OrmModel
        from fox_orm.relations import ManyToMany

        metadata = MetaData()

        class RelA(OrmModel):
            __metadata__ = metadata
            pkey: Optional[int] = pkey
            b_objs: ManyToMany['B'] = ManyToMany(to='placeholder', via='mid')

        class RelB(OrmModel):
            __metadata__ = metadata
            pkey: Optional[int] = pkey
            a_objs: ManyToMany['A'] = ManyToMany(to=RelA, via='mid')

        RelA.__relations__['b_objs']._to = RelB

        FoxOrm.init_relations(metadata)

        self.assertIn('mid', metadata.tables)
        self.assertNotIn('b_objs', metadata.tables['rel_a'].columns)
        self.assertNotIn('a_objs', metadata.tables['rel_b'].columns)
        self.assertEqual(
            schema_to_set(metadata.tables['mid']),
            {
                ('rel_a_id', Integer),
                ('rel_b_id', Integer),
            },
        )

    async def test_connection(self):
        from fox_orm import OrmModel

        conn = Connection()

        class TestConnection(OrmModel, connection=conn):
            pkey: int = pkey

        self.assertEqual(TestConnection.__connection__, conn)

    async def test_jsonb_generation(self):
        conn = Connection()

        class JsonbTypesHelper(BaseModel):
            a: str
            b: int

        class JsonbTypes(OrmModel, connection=conn):
            pkey: Optional[int] = pkey
            dict_: dict = jsonb
            list_: list = jsonb
            implicit_json: JsonbTypesHelper = jsonb

        self.assertEqual(
            schema_to_set(JsonbTypes.__table__),
            {
                ('pkey', sa.Integer),
                ('dict_', JSONB),
                ('list_', JSONB),
                ('implicit_json', JSONB),
            },
        )

    async def test_insert(self):
        a_inst = A(text='test', n=0)
        await a_inst.save()
        self.assertIsNotNone(a_inst.id)
        b_inst = B(text2='test2', n=0)
        await b_inst.save()
        self.assertIsNotNone(b_inst.id)

    async def test_datetime(self):
        dt = datetime.datetime.now()
        inst = E(dt=dt)
        await inst.save()
        self.assertIsNotNone(inst.pkey)
        inst = await E.get(inst.pkey)
        self.assertEqual(inst.dt, dt)

    async def test_select(self):
        inst1 = await A(text='test_select_1', n=0).save()
        inst2 = await A(text='test_select_2', n=2).save()
        inst3 = await A(text='test_select_3', n=4).save()

        selected = await A.select(A.text == 'test_select_2').all()
        self.assertEqual(len(selected), 1)
        selected = selected[0]
        self.assertEqual(selected.id, inst2.id)
        self.assertEqual(selected.text, 'test_select_2')

        selected = await A.select().where(A.text == 'test_select_3').first()
        self.assertEqual(selected.id, inst3.id)
        self.assertEqual(selected.text, 'test_select_3')

        selected = await A.select().where(text='test_select_3').first()
        self.assertEqual(selected.id, inst3.id)
        self.assertEqual(selected.text, 'test_select_3')

        selected = await A.select(
            A.__table__.select().where(A.text == 'test_select_1')
        ).first()
        self.assertEqual(selected.id, inst1.id)
        self.assertEqual(selected.text, 'test_select_1')

        selected = (
            await A.select()
            .where(A.text.ilike('test\_select\__'))
            .where(A.n >= 2)
            .all()
        )
        self.assertEqual(len(selected), 2)
        self.assertEqual({selected[0].id, selected[1].id}, {inst2.id, inst3.id})

        selected = await A.select().where(A.text == 'this_does_not_exist').first()
        self.assertIsNone(selected)

    async def test_select_values(self):
        inst = await A(text='test_select_values', n=0).save()

        selected = (
            await A.select('select * from a where text = :text')
            .values(text='test_select_values')
            .first()
        )
        self.assertEqual(selected.id, inst.id)

        selected = (
            await A.select('select * from a where text = :text')
            .values({'text': 'test_select_values'})
            .first()
        )
        self.assertEqual(selected.id, inst.id)

    async def test_get(self):
        inst = await A(text='test_get', n=0).save()
        selected = await A.get(inst.id)
        self.assertIsNotNone(selected)
        self.assertEqual(selected.id, inst.id)

    async def test_update(self):
        inst = A(text='test_update', n=0)
        await inst.save()
        inst.text = 'test_update2'
        await inst.save()
        selected = await A.select(A.text == 'test_update2').first()
        self.assertIsNotNone(selected)

    async def test_empty_update(self):
        inst = A(text='test_empty_update', n=1874)
        await inst.save()
        await inst.save()
        selected = await A.select(A.text == 'test_empty_update').first()
        self.assertEqual(selected.id, inst.id)
        self.assertEqual(selected.text, 'test_empty_update')
        self.assertEqual(selected.n, 1874)

    async def test_m2m(self):
        a_inst = A(text='test_m2m', n=0)
        await a_inst.save()
        b_inst = B(text2='test_m2m', n=0)
        await b_inst.save()
        await a_inst.fetch_related('b_objs')
        self.assertEqual(len(a_inst.b_objs), 0)
        await a_inst.b_objs.add(b_inst)
        await b_inst.fetch_related('a_objs')
        self.assertEqual(len(b_inst.a_objs), 1)
        self.assertEqual(b_inst.a_objs[0].text, 'test_m2m')

    async def test_m2m_2(self):
        a_inst = A(text='test_m2m_2', n=0)
        await a_inst.save()
        b_inst = B(text2='test_m2m_2_bad', n=0)
        with self.assertRaises(OrmError):
            a_inst.b_objs.add(b_inst)

        for i in range(10):
            b_inst = B(text2='test_m2m_2_' + str(i), n=0)
            await b_inst.save()
            a_inst.b_objs.add(b_inst)
        self.assertEqual(await a_inst.b_objs.count(), 0)
        await a_inst.b_objs.save()
        self.assertEqual(await a_inst.b_objs.count(), 10)
        await a_inst.b_objs.fetch()
        self.assertEqual(len(a_inst.b_objs), 10)
        self.assertEqual(await a_inst.b_objs.count(), 10)

    async def test_m2m_contains(self):
        a_inst = A(text='test_m2m_contains', n=0)
        await a_inst.save()
        await a_inst.b_objs.fetch()

        for i in range(10):
            b_inst = B(text2='test_m2m_contains_' + str(i), n=0)
            await b_inst.save()
            a_inst.b_objs.add(b_inst)
            a_inst.b_objs.add(b_inst)
        last_id = a_inst.b_objs[-1].id
        self.assertIn(last_id, a_inst.b_objs)
        self.assertIn(a_inst.b_objs[-1], a_inst.b_objs)
        b_inst = await B.select((B.c.text2 == 'test_m2m_contains_0') & (B.c.n == 0))
        self.assertIn(b_inst, a_inst.b_objs)

    async def test_m2m_delete(self):
        a_inst = A(text='test_m2m_delete', n=0)
        await a_inst.save()
        await a_inst.b_objs.fetch()

        for i in range(10):
            b_inst = B(text2='test_m2m_delete_' + str(i), n=0)
            await b_inst.save()
            a_inst.b_objs.add(b_inst)
            a_inst.b_objs.add(b_inst)
        await a_inst.b_objs.save()
        self.assertEqual(len(a_inst.b_objs), 10)

        b_inst = await B.select(B.c.text2 == 'test_m2m_delete_3')
        a_inst.b_objs.delete(b_inst)
        a_inst.b_objs.delete(b_inst)
        self.assertNotIn(b_inst, a_inst.b_objs)
        a_inst_2 = await A.select(A.c.text == 'test_m2m_delete')
        await a_inst_2.b_objs.fetch()
        self.assertIn(b_inst, a_inst_2.b_objs)

        await a_inst.b_objs.save()
        await a_inst_2.b_objs.fetch()
        self.assertNotIn(b_inst, a_inst_2.b_objs)

    async def test_bad_operation(self):
        user_inst = User(username='test_bad_operation')
        await user_inst.save()
        with self.assertRaises(ValueError):
            user_inst.groups = 1874
        with self.assertRaises(OrmError):
            user_inst.id = 1874
        with self.assertRaises(OrmError):
            await user_inst.fetch_related('non_existent_column')
        with self.assertRaises(OrmError):
            await user_inst.fetch_related('id')

    async def test_bad_model(self):
        from fox_orm import OrmModel

        with self.assertRaises(NoPrimaryKeyError):

            class BadModel1(OrmModel):
                pass

        with self.assertRaises(UnannotatedFieldError):

            class BadModel2(OrmModel):
                pkey: int = pkey
                kur = '123'

        with self.assertRaises(PrivateColumnError):

            class BadModel3(OrmModel):
                __smth__ = 'bad_model_3'
                pkey: int = pkey

        with self.assertRaises(PrivateColumnError):

            class BadModel4(OrmModel):
                __smth = 'bad_model_4'
                pkey: int = pkey

    async def test_default(self):
        TEST_KEY = 'test_default'
        inst = User(username=TEST_KEY)
        await inst.save()
        inst = await User.select(User.username == TEST_KEY).first()
        self.assertEqual(inst.balance, 100)
        self.assertEqual(inst.bio, 'a few words about me')

    async def test_composite_pkey(self):
        TEST_KEY = 'test_composite_pkey'
        user = await User(username=TEST_KEY).save()
        group = await Group(name=TEST_KEY).save()
        message = await Message(
            group_id=group.id, sender_id=user.id, text=TEST_KEY
        ).save()
        message = await Message.get(group.id, message.message_id)
        self.assertEqual(message.text, TEST_KEY)

    async def test_select_all(self):
        TEST_KEY = 'test_select_all'
        for i in range(10):
            inst = User(username=TEST_KEY, balance=1874)
            await inst.save()

        objs = await User.select(User.username == TEST_KEY).all()
        self.assertEqual(len(objs), 10)
        for obj in objs:
            self.assertEqual(obj.balance, 1874)

    async def test_order_by(self):
        for i in range(10):
            await A(text='test_order_by', n=i).save()

        objs = await A.select(A.text == 'test_order_by').order_by(A.n).all()
        self.assertEqual(len(objs), 10)
        for i, obj in enumerate(objs):
            self.assertEqual(obj.text, 'test_order_by')
            self.assertEqual(obj.n, i)

    async def test_delete(self):
        for i in range(10):
            await A(text='test_delete', n=i).save()
        await A.delete_where(A.text == 'test_delete')
        objs = await A.select(A.text == 'test_delete').all()
        self.assertEqual(len(objs), 0)

    async def test_delete_inst(self):
        inst = A(text='test_delete_inst', n=0)
        await inst.save()
        await inst.delete()
        res = await A.select(A.c.text == 'test_delete_inst').first()
        self.assertIsNone(res)

    async def test_o2m(self):
        b_inst = B(text2='test_o2m', n=0)
        await b_inst.save()
        c_inst = C()
        await c_inst.save()
        await b_inst.c_objs.fetch()
        self.assertEqual(len(b_inst.c_objs), 0)
        self.assertEqual(await b_inst.c_objs.count(), 0)
        c_inst.b_id = b_inst.id
        await c_inst.save()
        self.assertEqual(await b_inst.c_objs.count(), 1)
        self.assertEqual(len(b_inst.c_objs), 0)
        await b_inst.c_objs.fetch()
        self.assertEqual(len(b_inst.c_objs), 1)
        c_inst_2 = C()
        await c_inst_2.save()
        await b_inst.c_objs.add(c_inst_2)
        c_inst_2 = await C.get(c_inst_2.id)
        self.assertEqual(c_inst_2.b_id, b_inst.id)
        b_inst = await B.get(b_inst.id)
        await b_inst.c_objs.fetch()
        self.assertEqual(len(b_inst.c_objs), 2)
        self.assertEqual(await b_inst.c_objs.count(), 2)

    async def test_o2m_or_and(self):
        b_inst = B(text2='test_o2m_or_and', n=0)
        await b_inst.save()
        d_inst = D()
        await d_inst.save()
        c_inst_1 = C()
        c_inst_1.d_id = d_inst.pkey
        c_inst_1.b_id = b_inst.id
        await c_inst_1.save()
        c_inst_2 = C()
        c_inst_2.d_id = d_inst.pkey
        await c_inst_2.save()
        c_inst_3 = C()
        c_inst_3.b_id = b_inst.id
        await c_inst_3.save()
        await b_inst.c_objs.fetch()
        await d_inst.c_objs.fetch()
        self.assertEqual(len(d_inst.c_objs), 2)
        self.assertEqual(len(b_inst.c_objs), 2)
        self.assertEqual(len(b_inst.c_objs | d_inst.c_objs), 3)
        self.assertEqual(len(b_inst.c_objs & d_inst.c_objs), 1)
        both_have = (b_inst.c_objs & d_inst.c_objs)[0]
        self.assertEqual(both_have.id, c_inst_1.id)

    async def test_custom_id(self):
        inst = A(pkey=1874, text='test_custom_id', n=1)
        await inst.save()
        self.assertEqual(inst.id, 1874)
        await A.get(1874)

        inst_2 = A(text='test_custom_id', n=2)
        inst_2.id = 1875
        await inst_2.save()
        await A.get(1875)

    async def test_pydantic_support(self):
        inst = A(text='test_pydantic_support', n=1)
        await inst.save()
        inst.pydantic = PydanticTest(a=[PydanticTest2(a='123')])
        await inst.save()
        inst = await A.get(inst.id)

    async def test_inheritance(self):
        from sqlalchemy import Integer, JSON
        from fox_orm import OrmModel

        conn = Connection()

        class Test(OrmModel, connection=conn):
            pkey: Optional[int] = pkey
            test: str

        class TestInherited(Test, connection=conn):
            test: int
            test2: dict

        proper_schema = {
            ('pkey', Integer),
            ('test', Integer),
            ('test2', JSON),
        }
        self.assertEqual(TestInherited.__table__.name, 'test_inherited')
        self.assertEqual(schema_to_set(TestInherited.__table__), proper_schema)

    async def test_abstract(self):
        from sqlalchemy import Integer
        from fox_orm import OrmModel

        conn = Connection()

        class Test(OrmModel, abstract=True, connection=conn):
            pkey: Optional[int] = pkey

        class TestInherited(Test, connection=conn):
            test: int

        proper_schema = {
            ('pkey', Integer),
            ('test', Integer),
        }
        self.assertEqual(TestInherited.__table__.name, 'test_inherited')
        self.assertEqual(schema_to_set(TestInherited.__table__), proper_schema)

        with self.assertRaises(OrmError):
            Test(pkey=123)

        TestInherited(pkey=123, test=456)
