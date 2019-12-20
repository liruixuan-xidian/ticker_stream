# python 3.7

import asyncio
import websockets
import time
import json
import zlib
import pymongo
from send_text import send_message


binance_url = 'wss://stream.binance.com:9443/ws/btcusdt@bookTicker'
okex_url = 'wss://real.okex.com:8443/ws/v3'

okex_params = [{"op": "subscribe", "args": ["spot/candle86400s:BTC-USDT"]},
            {"op": "subscribe", "args": ["spot/candle86400s:ETH-USDT"]},
            {"op": "subscribe", "args": ["spot/candle86400s:EOS-USDT"]},
            {"op": "subscribe", "args": ["spot/candle86400s:BCH-USDT"]},
            {"op": "subscribe", "args": ["spot/candle86400s:BSV-USDT"]},
            {"op": "subscribe", "args": ["spot/candle86400s:TRX-USDT"]},
            {"op": "subscribe", "args": ["spot/candle86400s:XRP-USDT"]},
            {"op": "subscribe", "args": ["spot/candle86400s:LTC-USDT"]},
            {"op": "subscribe", "args": ["spot/candle86400s:ETC-USDT"]}
        ]
#okex_params_json = json.dumps(okex_params)
#print(okex_params_json)

def inflate(data): 
    decompress = zlib.decompressobj(
            -zlib.MAX_WBITS  # see above
    )
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated


async def get_avg_price(price1, price2):
    bb = float(price1)
    ba = float(price2)
    if bb and ba:
        return (bb + ba)/2
    return 0


async def parse_binance(resp_dict):
    data = {}
    print(resp_dict)
    price1 = resp_dict.get('b')     # best bid price
    price2 = resp_dict.get('a')     # best ask price
    data['market'] = 'BINANCE'
    instrumen = resp_dict.get('s')
    data['instrument'] = '{}-{}'.format(instrumen[:3], instrumen[3:])
    data['avg_price'] = await get_avg_price(price1, price2)
    return data


async def parse_okex(resp_dict):
    data = {}
    try:
        resp_dict_data = resp_dict.get('data')[0]
        price1 = resp_dict_data.get('best_bid')
        price2 = resp_dict_data.get('best_ask')
        data['market'] = 'OKEX'
        data['instrument'] = resp_dict_data.get('instrument_id')
        data['avg_price'] = await get_avg_price(price1, price2)
        return data
    except IndexError as e:
        return str(e)


async def print_result(url, resp_dict):
    if 'binance' in url:
        parsed_data = await parse_binance(resp_dict)
    else:
        parsed_data = await parse_okex(resp_dict)
    print(parsed_data)
    return
    if type(parsed_data) is dict:
        print('{} "{}" {} avg_price: $ {}'.format(time.strftime('%H:%M:%S'),
                                              parsed_data.get('market'),
                                              parsed_data.get('instrument'),
                                              parsed_data.get('avg_price')))
    else:
        print(parsed_data)


async def start_listening_to_binance(url):
    async with websockets.connect(url) as socket:
        while True:
            response = await socket.recv()

            resp_dict = json.loads(response)
            print(resp_dict)

#            await print_result(url, resp_dict)
            await asyncio.sleep(1)      # optional

async def send_mes(resp_dict, change_rate):
    resp_dict["change_rate"] = change_rate
    instrument = resp_dict["data"][0]["instrument_id"]
    open_price = resp_dict["data"][0]["candle"][1]
    close_price = resp_dict["data"][0]["candle"][4]
    body = "%s-%s-%s-%s" % (instrument, open_price, close_price, str(change_rate))
    print(body)
    send_message(body)

threshold = 2
async def okex_kline_change_rate(resp_dict):
    print(resp_dict)
    resp_dict_data = resp_dict["data"][0]
    open_price = resp_dict_data["candle"][1]
    close_price = resp_dict_data["candle"][4]
    hour = time.localtime(time.time())[3]
    minute = time.localtime(time.time())[4]
    sec = time.localtime(time.time())[5]
    hour = (hour + 8) % 24
    global threshold
    if hour == 8 and minute == 0 and sec == 0:
        threshold = 2

    change_rate = (float(close_price) - float(open_price))/ float(open_price) * 100
    if (abs(change_rate) > threshold):
        print(change_rate)
        await send_mes(resp_dict, change_rate)
        threshold += 1
    

async def start_listening_to_okex(url, okex_params_json):
    async with websockets.connect(url) as socket:

        await socket.send(okex_params_json)
        await socket.recv()

        while True:
            response = await socket.recv()

            inflated_resp = inflate(response)
            resp_dict = json.loads(inflated_resp)
#            print(resp_dict)
            await okex_kline_change_rate(resp_dict)

#            await print_result(url, resp_dict)
            await asyncio.sleep(1)      # optional

async def listening_okex_main_coin(url, params):
    for para in params:
        print(11111111111111111)
        print(para)
        para_json = json.dumps(para)
        await start_listening_to_okex(url, para_json)



async def main():
#    task1 = asyncio.create_task(start_listening_to_binance(binance_url))
    task2 = asyncio.create_task(listening_okex_main_coin(okex_url, okex_params))

#    await asyncio.gather(task1, task2)
    await asyncio.gather(task2)

if __name__ == '__main__':
    print('Listening is started...')
    asyncio.run(main())
