#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 30 22:42:39 2026

@author: twi-dev
"""

from massive import WebSocketClient
from massive.websocket.models import WebSocketMessage, Feed, Market
from typing import List

import logging

logging.basicConfig(level=logging.DEBUG)

client = WebSocketClient(
    api_key="_pukfD4W7DuMalk20qAyYLrj33DXlVAD",
    feed=Feed.Delayed,
    market=Market.Stocks
)

client.subscribe("AM.AAPL") # single ticker

def handle_msg(msgs: List[WebSocketMessage]):
    for m in msgs:
        logging.info(m)

client.run(handle_msg)
