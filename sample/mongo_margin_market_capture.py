
import asyncio
import loguru
from motor import motor_asyncio
from datetime import datetime

from pycoinglass import API

async def main(args):
    logger = loguru.logger
    logger.add(args.log, rotation="10MB", retention=3)

    mongo = motor_asyncio.AsyncIOMotorClient()
    db = mongo[args.db]
    collection = db[args.collection + "." + args.symbol + "." + args.perp_or_future]

    api = API(args.api_key)

    while True:
        df = api.margin_market_capture(args.symbol, perp_or_future=args.perp_or_future)
        d = df.to_dict('list')
        d['created_at'] = datetime.utcnow()
        logger.info(d)
        await collection.insert_one(d)
        await asyncio.sleep(args.interval)

if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument("--interval", default=15)
    parser.add_argument("--db", default="coinglass")
    parser.add_argument("--collection", default="margin_market_capture")
    parser.add_argument("--symbol", default="BTC")
    parser.add_argument("--perp_or_future", default="future", choices=["perp", "future"])
    parser.add_argument("--log", default="margin_market_capture.log")
    parser.add_argument("--api_key", default=None)
    args = parser.parse_args()
    asyncio.run(main(args))
