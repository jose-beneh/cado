"""Enable [`uvloop`](https://pypi.org/project/uvloop) as the event loop for `asyncio`."""

import asyncio

import uvloop

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
