
import asyncio
import pymongo
import loguru
from motor import motor_asyncio
from datetime import datetime

from pycoinglass import API

async def main(args):
    logger = loguru.logger
    logger.add(args.log, rotation="10MB", retention=3)

    mongo = motor_asyncio.AsyncIOMotorClient()
    db = mongo[args.db]
    collection = db[args.collection + "." + args.symbol]
    collection.create_index([("timestamp", pymongo.ASCENDING)], background=True, unique=True)

    api = API(args.api_key)

    while True:
        df = api.liquidation_chart(args.symbol, period="1m")

        d_exchanges = df.exchange.reset_index().groupby('timestamp').apply(lambda x: x.to_dict('records'))

        for d_total, d_exchange in zip(df.total.to_dict('records'), d_exchanges):
            ts = d_exchange[0]['timestamp']
            data = {'Total': d_total}
            for d in d_exchange:
                d.pop('timestamp')
                exc = d.pop('exchange')
                data[exc] = d

            d = {'timestamp': ts, 'data': data}
            ret = await collection.find_one_and_update(
                {'timestamp': ts}, {'$set': {'data': data}}, upsert=True
            )
            msg = "Register" if d is None else "Update"
            logger.info(f"{msg} {data}")

        await asyncio.sleep(args.interval)

if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument("--interval", default=300)
    parser.add_argument("--db", default="coinglass")
    parser.add_argument("--collection", default="liquidation")
    parser.add_argument("--symbol", default="BTC")
    parser.add_argument("--log", default="liquidation.log")
    parser.add_argument("--api_key", default=None)
    args = parser.parse_args()
    asyncio.run(main(args))
