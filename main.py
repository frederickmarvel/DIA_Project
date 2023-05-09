require('dotenv').config();
import asyncio
import json
import websockets
import pandas as pd
import numpy as np
import hmac
import base64
import time
from typing import List, Dict
from aiohttp import ClientSession
from datetime import datetime

api_key = process.env.API_KEY_OKX;
api_secret = process.env.SECRET_KEY_OKX;
api_passphrase = process.env.API_PASSPHRASE_OKX;

url = 'wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999'

async def create_signed_request(method: str, endpoint: str, params: dict = None, body: dict = None) -> dict:
    timestamp = str(time.time())
    message = timestamp + method.upper() + endpoint
    if params:
        message += "?" + "&".join([f"{k}={v}" for k, v in params.items()])
    if body:
        message += json.dumps(body)

    mac = hmac.new(api_secret.encode(), message.encode(), 'sha256')
    signature = base64.b64encode(mac.digest()).decode()

    headers = {
        "OK-ACCESS-KEY": api_key,
        "OK-ACCESS-SIGN": signature,
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": api_passphrase,
        "Content-Type": "application/json"
    }

    return {"headers": headers, "url": endpoint, "params": params, "body": body}

async def request(method: str, endpoint: str, params: dict = None, body: dict = None) -> dict:
    signed_request = await create_signed_request(method, endpoint, params, body)
    url = "https://www.okx.com" + signed_request["url"]

    async with ClientSession() as session:
        async with session.request(
            method,
            url,
            headers=signed_request["headers"],
            params=signed_request["params"],
            json=signed_request["body"]
        ) as response:
            return await response.json()

class CandlestickData:
    def __init__(self, columns: List[str]):
        self.df = pd.DataFrame(columns=columns)

    def add_candle(self, candle: Dict[str, any]):
        self.df = self.df.append(candle, ignore_index=True)

    def get_latest_data(self, n: int = 60):
        return self.df.tail(n)

def calculate_vwap(df: pd.DataFrame) -> float:
    q = df["volume"]
    p = (df["high"] + df["low"] + df["close"]) / 3
    return (p * q).sum() / q.sum()

def calculate_bollinger_bands(df: pd.DataFrame, window: int, num_std: float) -> Dict[str, float]:
    rolling_mean = df['close'].rolling(window=window).mean()
    rolling_std = df['close'].rolling(window=window).std()
    upper_band = rolling_mean + (rolling_std * num_std)
    lower_band = rolling_mean - (rolling_std * num_std)

    return {"upper_band": upper_band.iloc[-1], "lower_band": lower_band.iloc[-1]}

async def process_data_and_execute_trades(data: Dict) -> None:
    if "arg" in data and "candle" in data["arg"]["channel"]:
        candle = {
            "timestamp": datetime.utcfromtimestamp(data["data"][0]),
            "open": float(data["data"][1]),
            "high": float(data["data"][2]),
            "low": float(data["data"][3]),
            "close": float(data["data"][4]),
            "volume": float(data["data"][5]),
        }
        print(f"Received candle: {candle}")  
        candlestick_data.add_candle(candle)

        if len(candlestick_data.df) >= 60:
            latest_data = candlestick_data.get_latest_data()
            vwap = calculate_vwap(latest_data)
            bollinger_bands = calculate_bollinger_bands(latest_data, 20, 2)

            current_price = candle["close"]
            print(f"Current price: {current_price}")
            print(f"VWAP: {vwap}")
            print(f"Bollinger Bands: {bollinger_bands}")
            
            if current_price < bollinger_bands["lower_band"] and current_price < vwap:
                print("Buy Signal")
                # Execute a buy order
                await execute_order("buy", current_price)

            elif current_price > bollinger_bands["upper_band"] and current_price > vwap:
                print("Sell Signal")
                # Execute a sell order
                await execute_order("sell", current_price)

async def execute_order(side: str, price: float, size: float = 0.01) -> None:
    method = "POST"
    endpoint = "/api/v5/trade/order"
    body = {
        "instId": "BTC-USDT",
        "tdMode": "cash",
        "side": side.upper(),
        "ordType": "limit",
        "px": str(price),
        "sz": str(size),
        "clOrdId": str(int(time.time() * 1000))  
    }

    response = await request(method, endpoint, body=body)
    

async def subscribe_authenticated(url: str, channels: List[Dict]):
    signed_request = await create_signed_request("GET", "/users/self/verify")
    headers = signed_request["headers"]

    async with websockets.connect(url, extra_headers=headers) as websocket:
        for channel in channels:
            await websocket.send(json.dumps(channel))

        while True:
            response = await websocket.recv()
            print(f"Received WebSocket message: {response}")  
            data = json.loads(response)

            if 'data' in data:
                await process_data_and_execute_trades(data)
                
async def subscribe(url: str, channels: List[Dict]):
    async with websockets.connect(url) as websocket:
        print("Connected to WebSocket")  
        for channel in channels:
            await websocket.send(json.dumps(channel))
        while True:
            response = await websocket.recv()
            data = json.loads(response)

            if 'data' in data:
                await process_data_and_execute_trades(data)
def main():
    channels = [
        {
            "op": "subscribe",
            "args": ["candle60s:BTC-USDT"]
        }
    ]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(subscribe(url, channels))

if __name__ == '__main__':
    candlestick_data = CandlestickData(["timestamp", "open", "high", "low", "close", "volume"])
    main()

       
