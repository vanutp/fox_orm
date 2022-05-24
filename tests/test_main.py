import datetime
import os
import unittest
from typing import List, Optional, Dict

from sqlalchemy import create_engine, Column, ForeignKey

from fox_orm import FoxOrm, Connection
from fox_orm.exceptions import *
from fox_orm.column.flags import fkey, null, index, autoincrement, unique
from fox_orm.relations import ManyToMany
from tests.models import A, B, C, D, RecursiveTest, RecursiveTest2, E
from tests.utils import schema_to_set

DB_FILE = 'test.db'
DB_URI = 'sqlite:///test.db'


class TestMain(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)
        FoxOrm.connections['default'] = Connection()
        FoxOrm.init(DB_URI)
        self.engine = create_engine(DB_URI)
        FoxOrm.metadata.create_all(self.engine)

    async def test_table_generation(self):
        from datetime import datetime, date, time, timedelta
        from pydantic import BaseModel
        from sqlalchemy import Integer, Float, String, Boolean, DateTime, Date, Time, Interval, JSON
        from fox_orm import OrmModel
        from fox_orm.column.flags import pk

        class AllTypesHelper(BaseModel):
            a: str
            b: int

        class AllTypesRelHelper(OrmModel):
            pkey: int = pk

        class AllTypes(OrmModel):
            pkey: Optional[int] = pk
            int_: int = null
            float_: float = ~null
            str_: Optional[str]
            bool_: bool = autoincrement

            datetime_: datetime = index
            date_: date = unique
            time_: time
            timedelta_: timedelta

            dict_: dict
            list_: list
            typed_dict: Dict[str, str]
            typed_list: List[int]
            implicit_json: AllTypesHelper

            foreign_key: int = fkey('all_types_rel_helper.pkey')

        all_types_proper_schema = {
            ('pkey', Integer),
            ('int_', Integer),
            ('float_', Float),
            ('str_', String),
            ('bool_', Boolean),
            ('datetime_', DateTime),
            ('date_', Date),
            ('time_', Time),
            ('timedelta_', Interval),
            ('dict_', JSON),
            ('list_', JSON),
            ('typed_dict', JSON),
            ('typed_list', JSON),
            ('implicit_json', JSON),
            ('foreign_key', Integer),
        }
        self.assertEqual(schema_to_set(AllTypes.__table__), all_types_proper_schema)
        self.assertFalse(AllTypes.pkey.nullable)
        self.assertTrue(AllTypes.int_.nullable)
        self.assertFalse(AllTypes.float_.nullable)
        self.assertTrue(AllTypes.str_.nullable)
        self.assertTrue(AllTypes.bool_.autoincrement)
        self.assertTrue(AllTypes.datetime_.index)
        self.assertTrue(AllTypes.date_.unique)
        self.assertEqual(list(AllTypes.__table__.c.foreign_key.foreign_keys)[0].column, AllTypesRelHelper.pkey)

    async def test_relationship_generation(self):
        from sqlalchemy import MetaData, Integer
        from fox_orm import OrmModel
        from fox_orm.column.flags import pk
        from fox_orm.relations import ManyToMany

        metadata = MetaData()

        class RelA(OrmModel):
            __metadata__ = metadata
            pkey: Optional[int] = pk
            b_objs: ManyToMany['B'] = ManyToMany(to='placeholder', via='mid')

        class RelB(OrmModel):
            __metadata__ = metadata
            pkey: Optional[int] = pk
            a_objs: ManyToMany['A'] = ManyToMany(to=RelA, via='mid')

        RelA.__relations__['b_objs']._to = RelB

        FoxOrm.init_relations(metadata)

        self.assertIn('mid', metadata.tables)
        self.assertNotIn('b_objs', metadata.tables['rel_a'].columns)
        self.assertNotIn('a_objs', metadata.tables['rel_b'].columns)
        self.assertEqual(schema_to_set(metadata.tables['mid']),
                         {
                             ('rel_a_id', Integer),
                             ('rel_b_id', Integer),
                         })

    async def test_jsonb_generation(self):
        from sqlalchemy import MetaData, Integer
        from sqlalchemy.dialects.postgresql import JSONB
        from pydantic import BaseModel
        from fox_orm import OrmModel
        from fox_orm.column.flags import pk, jsonb

        metadata = MetaData()

        class JsonbTypesHelper(BaseModel):
            a: str
            b: int

        class JsonbTypes(OrmModel):
            __metadata__ = metadata
            pkey: Optional[int] = pk
            dict_: dict = jsonb
            list_: list = jsonb
            implicit_json: JsonbTypesHelper = jsonb

        self.assertEqual(JsonbTypes.__table__.metadata, metadata)
        self.assertEqual(schema_to_set(JsonbTypes.__table__),
                         {
                             ('pkey', Integer),
                             ('dict_', JSONB),
                             ('list_', JSONB),
                             ('implicit_json', JSONB),
                         })

    async def test_insert(self):
        a_inst = A(text='test', n=0)
        await a_inst.save()
        self.assertIsNotNone(a_inst.pkey)
        b_inst = B(text2='test2', n=0)
        await b_inst.save()
        self.assertIsNotNone(b_inst.pkey)

    async def test_datetime(self):
        dt = datetime.datetime.now()
        inst = E(dt=dt)
        await inst.save()
        self.assertIsNotNone(inst.pkey)
        inst = await E.get(inst.pkey)
        self.assertEqual(inst.dt, dt)

    async def test_select(self):
        inst = A(text='test_select', n=0)
        await inst.save()
        inst = await A.select(A.c.text == 'test_select')
        self.assertIsNotNone(inst)
        self.assertEqual(inst.text, 'test_select')

    async def test_update(self):
        a_inst = A(text='test_update', n=0)
        await a_inst.save()
        a_inst.text = 'test_update2'
        await a_inst.save()
        a_inst = await A.select(A.c.text == 'test_update2')
        self.assertIsNotNone(a_inst)

    async def test_empty_update(self):
        inst = A(text='test_empty_update', n=0)
        await inst.save()
        await inst.save()

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
        with self.assertRaises(OrmException):
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
        last_id = a_inst.b_objs[-1].pkey
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
        a_inst = A(text='test_bad_operation', n=0)
        await a_inst.save()
        with self.assertRaises(ValueError):
            a_inst.b_objs = 1874
        with self.assertRaises(OrmException):
            a_inst.pkey = 1874
        with self.assertRaises(OrmException):
            await a_inst.fetch_related('pkey')

    async def test_bad_model(self):
        from fox_orm import OrmModel
        from fox_orm.column.flags import pk

        with self.assertRaises(OrmException):
            class BadModel1(OrmModel):
                pass
        with self.assertRaises(OrmException):
            class BadModel2(OrmModel):
                pkey: Optional[int] = pk
                kur = '123'
        with self.assertRaises(OrmException):
            class BadModel3(OrmModel):
                __sqla_table__ = 'bad_model_3'
                pkey: Optional[int] = pk
        with self.assertRaises(OrmException):
            class BadModel4(OrmModel):
                __table__ = 'bad_model_4'
                pkey: Optional[int] = pk
        with self.assertRaises(OrmException):
            class BadModel5(OrmModel):
                __tablename__ = 123
                pkey: Optional[int] = pk
        with self.assertRaises(OrmException):
            class BadModel6(OrmModel):
                __metadata__ = 123
                pkey: Optional[int] = pk
        with self.assertRaises(OrmException):
            class BadModel7(OrmModel):
                pkey: Optional[int] = pk
                id2: Optional[int] = pk
        with self.assertRaises(OrmException) as exc:
            class BadModel8(OrmModel):
                pass

    async def test_select_all(self):
        for i in range(10):
            inst = A(text='test_select_all', n=1874)
            await inst.save()

        objs: List[A] = await A.select_all(A.c.n == 1874)
        self.assertEqual(len(objs), 10)
        for obj in objs:
            self.assertEqual(obj.text, 'test_select_all')

    async def test_select_nonexistent(self):
        res = await A.select_all(A.c.text == 'test_select_nonexistent')
        self.assertEqual(len(res), 0)
        res = await A.select(A.c.text == 'test_select_nonexistent')
        self.assertIsNone(res)
        res = await A.exists(A.c.text == 'test_select_nonexistent')
        self.assertFalse(res)

    async def test_exists(self):
        a_inst = A(text='test_exists', n=0)
        await a_inst.save()
        res = await A.exists(A.c.text == 'test_exists')
        self.assertTrue(res)

    async def test_count(self):
        for i in range(10):
            inst = A(text='test_count', n=1875)
            await inst.save()
        res = await A.count(A.c.n == 1875)
        self.assertEqual(res, 10)

    async def test_order_by(self):
        for i in range(10):
            inst = A(text='test_order_by', n=i)
            await inst.save()

        objs: List[A] = await A.select_all(A.c.text == 'test_order_by', order_by=A.c.n)
        self.assertEqual(len(objs), 10)
        i = 0
        for obj in objs:
            self.assertEqual(obj.text, 'test_order_by')
            self.assertEqual(obj.n, i)
            i += 1

    async def test_delete(self):
        for i in range(10):
            inst = A(text='test_delete', n=i)
            await inst.save()
        await A.delete(A.c.text == 'test_delete')
        objs = await A.select_all(A.c.text == 'test_delete')
        self.assertEqual(len(objs), 0)

    async def test_delete_inst(self):
        inst = A(text='test_delete_inst', n=0)
        await inst.save()
        await inst.delete()
        res = await A.exists(A.c.text == 'test_delete_inst')
        self.assertFalse(res)

    async def test_o2m(self):
        b_inst = B(text2='test_o2m', n=0)
        await b_inst.save()
        c_inst = C()
        await c_inst.save()
        await b_inst.c_objs.fetch()
        self.assertEqual(len(b_inst.c_objs), 0)
        self.assertEqual(await b_inst.c_objs.count(), 0)
        c_inst.b_id = b_inst.pkey
        await c_inst.save()
        self.assertEqual(await b_inst.c_objs.count(), 1)
        self.assertEqual(len(b_inst.c_objs), 0)
        await b_inst.c_objs.fetch()
        self.assertEqual(len(b_inst.c_objs), 1)
        c_inst_2 = C()
        await c_inst_2.save()
        await b_inst.c_objs.add(c_inst_2)
        c_inst_2 = await C.get(c_inst_2.pkey)
        self.assertEqual(c_inst_2.b_id, b_inst.pkey)
        b_inst = await B.get(b_inst.pkey)
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
        c_inst_1.b_id = b_inst.pkey
        await c_inst_1.save()
        c_inst_2 = C()
        c_inst_2.d_id = d_inst.pkey
        await c_inst_2.save()
        c_inst_3 = C()
        c_inst_3.b_id = b_inst.pkey
        await c_inst_3.save()
        await b_inst.c_objs.fetch()
        await d_inst.c_objs.fetch()
        self.assertEqual(len(d_inst.c_objs), 2)
        self.assertEqual(len(b_inst.c_objs), 2)
        self.assertEqual(len(b_inst.c_objs | d_inst.c_objs), 3)
        self.assertEqual(len(b_inst.c_objs & d_inst.c_objs), 1)
        both_have = (b_inst.c_objs & d_inst.c_objs)[0]
        self.assertEqual(both_have.pkey, c_inst_1.pkey)

    async def test_custom_id(self):
        inst = A(pkey=1874, text='test_custom_id', n=1)
        await inst.save()
        self.assertEqual(inst.pkey, 1874)
        await A.get(1874)

        inst_2 = A(text='test_custom_id', n=2)
        inst_2.pkey = 1875
        await inst_2.save()
        await A.get(1875)

    async def test_recursive_serialization(self):
        inst = A(text='test_recursive_serialization', n=1)
        await inst.save()
        inst.recursive = RecursiveTest(a=[RecursiveTest2(a='123')])
        await inst.save()

    async def test_extra_fields(self):
        inst = ExtraFields()
        inst._test = 123
        await inst.save()
        inst = await ExtraFields.get(inst.pkey)

    async def test_select_sqla_core(self):
        inst = A(text='test_select_sqla_core', n=0)
        await inst.save()
        inst = await A.select(A.__table__.select().where(A.c.text == 'test_select_sqla_core'))
        self.assertIsNotNone(inst)
        self.assertEqual(inst.text, 'test_select_sqla_core')

    async def test_select_raw_sql(self):
        inst = A(text='test_select_raw_sql', n=0)
        await inst.save()
        inst = await A.select('''select * from a where text = :text''', {'text': 'test_select_raw_sql'})
        self.assertIsNotNone(inst)
        self.assertEqual(inst.text, 'test_select_raw_sql')

    async def test_inheritance(self):
        from sqlalchemy import MetaData, Integer, JSON
        from fox_orm import OrmModel
        from fox_orm.column.flags import pk

        metadata = MetaData()

        class Test(OrmModel):
            __metadata__ = metadata
            pkey: Optional[int] = pk
            test: str

        class TestInherited(Test):
            __metadata__ = metadata
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
        from sqlalchemy import MetaData, Integer
        from fox_orm import OrmModel
        from fox_orm.column.flags import pk

        conn = FoxOrm.connections['test_abstract'] = Connection()

        class Test(OrmModel, abstract=True, connection=conn):
            pkey: Optional[int] = pk

        class TestInherited(Test, connection=conn):
            test: int

        proper_schema = {
            ('pkey', Integer),
            ('test', Integer),
        }
        self.assertEqual(TestInherited.__table__.name, 'test_inherited')
        self.assertEqual(schema_to_set(TestInherited.__table__), proper_schema)

        with self.assertRaises(OrmException):
            Test(pkey=123)
        with self.assertRaises(OrmException):
            Test.construct({'pkey': 123})

        TestInherited(pkey=123, test=456)
        TestInherited.construct({'pkey': 123, 'test': 456})

    async def test_getattribute(self):
        self.assertEqual(A.__table__.c.pkey, A.pkey)
        self.assertEqual(A.__table__.c.text, A.text)
        self.assertEqual(A.__table__.c.n, A.n)
        self.assertEqual(A.__table__.c.recursive, A.recursive)
        self.assertIsInstance(A.b_objs, ManyToMany)
        self.assertIs(A.b_objs._to, B)

