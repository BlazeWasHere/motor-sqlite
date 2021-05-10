#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio

from src.api import MotorSqlite


async def test():
    """
    This test function assumes you have a sqlite .db file in data/
    called `test.db` with a table called `posts` with a column called `key`
    with an existing `key` with the value `exists`
    """

    db = database.test
    posts = db.posts

    # None
    print(await posts.find_one({'key': 'foo'}))

    # {'key': 'exists'}
    print(await posts.find_one({'key': 'exists'}))

    # <async_generator object MotorSqliteTable.find at 0xxxxxxxxxxxxx>
    print(posts.find({'key': 'exists'}))

    # {'key': 'exists'}
    async for x in posts.find({'key': 'exists'}):
        print(x)

    # TODO: update(...) func
    await posts.update(
        {'foo': 'bar'},
        {'$set': {
            'val': 'key',
            'key': 'val',
        },
    })


database = MotorSqlite()

asyncio.run(test())
