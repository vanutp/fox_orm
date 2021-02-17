import os
import unittest
from typing import List

from pydantic import ValidationError
from sqlalchemy import *

from fox_orm import FoxOrm
from fox_orm.exceptions import *
from tests.models import metadata, A, B

DB_FILE = 'test.db'
DB_URI = 'sqlite:///test.db'


class TestMain(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)
        FoxOrm.init(DB_URI)
        metadata.create_all(create_engine(DB_URI))

    async def test_insert(self):
        a_inst = A(text='test', n=0)
        await a_inst.save()
        self.assertIsNotNone(a_inst.id)
        b_inst = B(text2='test2', n=0)
        await b_inst.save()
        self.assertIsNotNone(b_inst.id)

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
        await a_inst.b_objs.save()
        with self.assertRaises(NotFetchedException):
            self.assertEqual(len(a_inst.b_objs), 10)
        await a_inst.b_objs.fetch()
        self.assertEqual(len(a_inst.b_objs), 10)

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
        b_inst = await B.select(and_(B.c.text2 == 'test_m2m_contains_0', B.c.n == 0))
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

    async def test_bad(self):
        a_inst = A(text='test_bad', n=0)
        await a_inst.save()
        with self.assertRaises(ValidationError):
            a_inst.b_objs = 1874
        with self.assertRaises(OrmException):
            a_inst.id = 1874

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
        self.assertEqual(res, False)

    async def test_exists(self):
        a_inst = A(text='test_exists', n=0)
        await a_inst.save()
        res = await A.exists(A.c.text == 'test_exists')
        self.assertEqual(res, True)

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

    async def test_bad_model(self):
        from fox_orm.model import OrmModel
        from typing import Optional
        with self.assertRaises(OrmException):
            class Model(OrmModel):
                id: Optional[int]

    async def test_delete(self):
        for i in range(10):
            inst = A(text='test_delete', n=i)
            await inst.save()
        await A.delete(A.c.text == 'test_delete')
        objs = await A.select_all(A.c.text == 'test_delete')
        self.assertEqual(len(objs), 0)
