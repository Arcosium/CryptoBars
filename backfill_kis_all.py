#!/usr/bin/env python3
"""전종목 백필 — backfill_kis.py 를 그대로 쓰되 유니버스만 바꾼다.

universe.csv 는 네이버 수집기(equities.py)도 읽는다. 거기에 2,565종목을 넣으면
5분 주기 크롤링이 초당 8.5요청으로 뛰어 차단 위험이 있으므로 목록을 분리했다.
"""
import asyncio, backfill_kis as bk

bk.UNIVERSE = '/home/arcosium/vault/CryptoBars/data/KRX/universe_all.csv'
asyncio.run(bk.main())
