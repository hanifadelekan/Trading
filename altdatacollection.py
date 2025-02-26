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
bid_df = None
ask_df = None
obevent_time = []

async def connect_to_binance():
    async with websockets.connect(WS_URL) as websocket:
        await on_open(websocket)
        while True:
            message = await websocket.recv()
            await on_message(websocket, message)
            if snapshot_count > 10:
                #ob_df.to_csv('order_book_data.csv', index=False)

                bid_df.to_parquet('bid_order_book.parquet', compression='zstd')
                ask_df.to_parquet('ask_order_book.parquet', compression='zstd')
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
    global snapshot_count, ob_lastupdateid, ob_snapshot, bid_df, ask_df, obevent_time

    event = ujson.loads(message)
    if 'result' in event:
        return  # Ignore subscription confirmation
    
    # Process order book update
    if snapshot_count == 0:
        # Fetch initial snapshot
        ob_snapshot = await fetch(REST_URL)
        ob_lastupdateid = ob_snapshot['lastUpdateId']
        obevent_time = ob_snapshot['E']
        bids = ob_snapshot['bids']
        asks = ob_snapshot['asks']
        bid_df = pd.DataFrame(bids, columns=['price', 'size'])
        bid_df.set_index('price', inplace=True)
        ask_df = pd.DataFrame(asks, columns=['price', 'size'])
        ask_df.set_index('price', inplace=True)
        bid_df['time'] = [obevent_time] * len(bid_df)
        ask_df['time'] = [obevent_time] * len(ask_df)
        bid_df['snapshot'] = [snapshot_count] * len(bid_df)
        ask_df['snapshot'] = [snapshot_count] * len(ask_df)
     

       
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
      
        oldbid_df = bid_df.copy()
        oldask_df = ask_df.copy()
        for price, size in ubids:
            if size == 0:
                bid_df.drop(price, errors='ignore')
            else:
                
                bid_df.loc[price] = [size, event_time,None]
        
        for price, size in uasks:
            #print(price,size)
            if size == 0:
                ask_df.drop(price, errors='ignore')
                
            else:
                ask_df.loc[price] = [size, event_time,None]
        bid_df['snapshot'] = [snapshot_count] * len(bid_df)
        ask_df['snapshot'] = [snapshot_count] * len(ask_df)
        bid_df = pd.concat([oldbid_df, bid_df])
        ask_df = pd.concat([oldask_df, ask_df])
        
        

        print(bid_df)
        
        
        ob_lastupdateid = event['u']
        snapshot_count += 1
        logging.info(f"Processed update {snapshot_count-1}")


async def fetch(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.json()

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(connect_to_binance())