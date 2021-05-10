#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
            Copyright Blaze 2021.
 Distributed under the Boost Software License, Version 1.0.
    (See accompanying file LICENSE or copy at
          https://www.boost.org/LICENSE_1_0.txt)
"""

from typing import Any, Dict, Generator, Iterable, Optional, Union

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
        return await self.db.execute(query, values)

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
