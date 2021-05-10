#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
            Copyright Blaze 2021.
 Distributed under the Boost Software License, Version 1.0.
    (See accompanying file LICENSE or copy at
          https://www.boost.org/LICENSE_1_0.txt)
"""

from typing import Any, Dict, Generator, Iterable, Optional, Union, Tuple, List

from aiosqlite.core import Connection
from aiosqlite.cursor import Cursor

from .utils import build_query, dict_factory


class MotorSqliteTable(object):
    def __init__(self, conn: Connection, table: str) -> None:
        self.db = None
        self.conn = conn
        self.table = table

        # SQL queries
        self.select = f"SELECT * FROM {table} WHERE "
        self.insert = f"UPDATE {table} SET "

    async def _execute(
        self, query: str, values: Iterable[Any] = None
    ) -> Cursor:

        if self.db is None:
            self.db = await self.conn

        self.db.row_factory = dict_factory  # type: ignore
        cursor = await self.db.execute(query, values)
        await self.db.commit()

        return cursor

    async def find(
        self, _dict: Dict[str, Any]
    ) -> Generator[Dict[str, Union[str, int]], None, None]:

        query, values = build_query(self.select, _dict)
        cursor = await self._execute(query, values)

        # async generator to keep it similar to motor api
        for res in await cursor.fetchall():
            yield res

    async def find_one(
        self, _dict: Dict[str, Any]
    ) -> Optional[Dict[str, Union[str, int]]]:

        try:
            return await self.find(_dict).__anext__()
        except StopAsyncIteration:
            return None

    def _update(
        self, _dict: Dict[str, Any], opts: Dict[str, Dict[str, Any]]
    ) -> Tuple[str, List[Any]]:
        query, values = build_query(self.insert, opts['$set'], " = ?,")
        # such an ugly way to remove trailing `,` and add `WHERE`
        query = query[:-1] + " WHERE "

        query, x = build_query(query, _dict)
        values += x

        return query, values

    async def update(
        self, _dict: Dict[str, Any], opts: Dict[str, Dict[str, Any]]
    ) -> int:
        query, values = self._update(_dict, opts)
        cursor = await self._execute(query, values)

        return cursor.rowcount

    async def update_one(
        self, _dict: Dict[str, Any], opts: Dict[str, Dict[str, Any]]
    ) -> bool:
        query, values = self._update(_dict, opts)
        query += " LIMIT 1"
        
        cursor = await self._execute(query, values)

        return cursor.rowcount == 1
