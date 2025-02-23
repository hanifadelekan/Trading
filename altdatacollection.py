import asyncio
import websockets
import json
import logging
import ujson
import aiohttp
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)

# Binance REST and WebSocket URLs
REST_URL = "https://fapi.binance.com/fapi/v1/depth?symbol=SOLUSDT&limit=1000"
WS_URL = "wss://fstream.binance.com/ws/solusdt@depth@100ms"

# Global variables
ob_snapshot = None
snapshot_count = 0
ob_lastupdateid = None
ob_df = pd.DataFrame(columns=['count', 'time', 'type', 'price', 'size', 'best_bid', 'best_ask', 'spread', 'midprice','length'])
obids = {}
oasks = {}
bid_df = None
ask_df = None
event_time = []

async def connect_to_binance():
    async with websockets.connect(WS_URL) as websocket:
        await on_open(websocket)
        while True:
            message = await websocket.recv()
            await on_message(websocket, message)
            if snapshot_count > 10:
                #ob_df.to_csv('order_book_data.csv', index=False)
                #ob_df.to_parquet('order_book.parquet', compression='zstd')
                logging.info("Saved 200 updates to CSV. Exiting.")
                print(bid_df)
                break

async def on_open(websocket):
    logging.info("WebSocket connection opened.")
    subscribe_message = {
        "method": "SUBSCRIBE",
        "params": ["solusdt@depth@100ms"],
        "id": 1
    }
    await websocket.send(json.dumps(subscribe_message))

async def on_message(websocket, message):
    global snapshot_count, ob_lastupdateid, ob_snapshot, ob_df, obids, oasks, bid_df, ask_df, event_time

    event = ujson.loads(message)
    if 'result' in event:
        return  # Ignore subscription confirmation
    
    # Process order book update
    if snapshot_count == 0:
        # Fetch initial snapshot
        ob_snapshot = await fetch(REST_URL)
        ob_lastupdateid = ob_snapshot['lastUpdateId']
        event_time = ob_snapshot['E']
        bids = ob_snapshot['bids']
        asks = ob_snapshot['asks']
        multi_indexb = pd.MultiIndex.from_arrays([[event_time] * len(bids),range(len(bids)), [x[0] for x in bids]], names=['timestamp', 'level','price'])
        bid_df = pd.DataFrame(bids, columns=['price','size'],index=multi_indexb)
        print(bid_df)
      
        ask_df = pd.DataFrame(asks, columns=['price', 'size'])
        # Initialize order book
        bid_df.set_index('price', inplace=True)
        ask_df.set_index('price', inplace=True)
       
        snapshot_count += 1
        logging.info("Initial snapshot processed")
    
    elif snapshot_count >= 1:
        # Check update sequence validity
        if snapshot_count == 1:
            # Verify first update matches snapshot
            if not (event['U'] <= ob_lastupdateid + 1 and event['u'] >= ob_lastupdateid + 1):
                return  # Skip until valid update
        
        # Process updates
        ubids = event['b']
        uasks = event['a']
        event_time = event['E']
        
        # Update order book
        for price, size in ubids:
            if size == 0:
                bid_df.drop(price, errors='ignore')
            else:
                bid_df.loc[price,'size'] = size
        for price, size in uasks:
            if size == 0:
                ask_df.drop(price, errors='ignore')
            else:
                ask_df.loc[price,'size'] = size
        
     
        ob_lastupdateid = event['u']
        snapshot_count += 1
        logging.info(f"Processed update {snapshot_count-1}")


async def fetch(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.json()

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(connect_to_binance())