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

async def connect_to_binance():
    async with websockets.connect(WS_URL) as websocket:
        await on_open(websocket)
        while True:
            message = await websocket.recv()
            await on_message(websocket, message)
            if snapshot_count > 20000:
                #ob_df.to_csv('order_book_data.csv', index=False)
                ob_df.to_parquet('order_book.parquet', compression='zstd')
                logging.info("Saved 200 updates to CSV. Exiting.")
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
    global snapshot_count, ob_lastupdateid, ob_snapshot, ob_df, obids, oasks

    event = ujson.loads(message)
    if 'result' in event:
        return  # Ignore subscription confirmation
    
    # Process order book update
    if snapshot_count == 0:
        # Fetch initial snapshot
        ob_snapshot = await fetch(REST_URL)
        ob_lastupdateid = ob_snapshot['lastUpdateId']
        event_time = ob_snapshot['E']
        
        # Initialize order book
        obids = {float(p[0]): float(p[1]) for p in ob_snapshot['bids']}
        oasks = {float(p[0]): float(p[1]) for p in ob_snapshot['asks']}
        
        # Create DataFrame entries
        await update_dataframe(event_time)
        snapshot_count += 1
        logging.info("Initial snapshot processed")
    
    elif snapshot_count >= 1:
        # Check update sequence validity
        if snapshot_count == 1:
            # Verify first update matches snapshot
            if not (event['U'] <= ob_lastupdateid + 1 and event['u'] >= ob_lastupdateid + 1):
                return  # Skip until valid update
        
        # Process updates
        ubids = {float(p[0]): float(p[1]) for p in event['b']}
        uasks = {float(p[0]): float(p[1]) for p in event['a']}
        event_time = event['E']
        
        # Update order book
        for price, size in ubids.items():
            if size == 0:
                obids.pop(price, None)
            else:
                obids[price] = size
        for price, size in uasks.items():
            if size == 0:
                oasks.pop(price, None)
            else:
                oasks[price] = size
        
        # Update dataframe
        await update_dataframe(event_time)
        ob_lastupdateid = event['u']
        snapshot_count += 1
        logging.info(f"Processed update {snapshot_count-1}")

async def update_dataframe(event_time):
    global ob_df
    
    sorted_bids = sorted(obids.items(), key=lambda x: -x[0])
    sorted_asks = sorted(oasks.items(), key=lambda x: x[0])
    
    best_bid = sorted_bids[0][0] if sorted_bids else None
    best_ask = sorted_asks[0][0] if sorted_asks else None
    
    # Create SINGLE ROW per update with full order book
    new_row = {
        'timestamp': event_time,
        'update_count': snapshot_count,
        'bids': ujson.dumps(sorted_bids),  # Store as JSON string
        'asks': ujson.dumps(sorted_asks),
        'best_bid': best_bid,
        'best_ask': best_ask,
        'spread': best_ask - best_bid,
        'midprice': (best_bid + best_ask)/2,
        'length': len(sorted_bids) + len(sorted_asks)
    }
    
    ob_df = pd.concat([ob_df, pd.DataFrame([new_row])], ignore_index=True)

async def fetch(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.json()

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(connect_to_binance())