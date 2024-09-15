import asyncio
import json
import logging
import threading
import websocket
import boto3
from botocore.exceptions import ClientError
import time
from decimal import Decimal

# DynamoDBの設定
REGION_NAME = 'ap-northeast-1'  # 東京リージョン
TABLE_NAME = 'stock_data'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
table = dynamodb.Table(TABLE_NAME)

async def store_pushdata_in_dynamodb(content):
    try:
        item = {
            'symbol': content['Symbol'],
            'get_time': content['CurrentPriceTime'],
            'current_price': Decimal(str(content['CurrentPrice'])),
            'HighPrice': Decimal(str(content['HighPrice'])),
            'VWAP': Decimal(str(content['VWAP'])),
            'LowPrice': Decimal(str(content['LowPrice'])),
            'TradingVolume': Decimal(str(content['TradingVolume'])),
            'TradingValue': Decimal(str(content['TradingValue'])),
            'BidQty': Decimal(str(content['BidQty'])),
            'BidPrice': Decimal(str(content['BidPrice'])),
            'MarketOrderSellQty': Decimal(str(content['MarketOrderSellQty'])),
            'AskQty': Decimal(str(content['AskQty'])),
            'AskPrice': Decimal(str(content['AskPrice'])),
            'MarketOrderBuyQty': Decimal(str(content['MarketOrderBuyQty'])),
            'OverSellQty': Decimal(str(content['OverSellQty'])),
            'UnderBuyQty': Decimal(str(content['UnderBuyQty'])),
            'SymbolName': content['SymbolName'],
            'CurrentPriceChangeStatus': content['CurrentPriceChangeStatus'],
            'ChangePreviousClose': Decimal(str(content['ChangePreviousClose'])),
            'OpeningPriceTime': content['OpeningPriceTime'],
        }

        # Sell1~Sell10, Buy1~Buy10のデータを追加
        for i in range(1, 11):
            sell_key = f'Sell{i}'
            buy_key = f'Buy{i}'
            if sell_key in content:
                item[f'{sell_key}_Price'] = Decimal(str(content[sell_key].get('Price', '0')))
                item[f'{sell_key}_Qty'] = Decimal(str(content[sell_key].get('Qty', '0')))
                item[f'{sell_key}_Sign'] = content[sell_key].get('Sign', 'N/A')
                item[f'{sell_key}_Time'] = content[sell_key].get('Time','N/A')
            if buy_key in content:
                item[f'{buy_key}_Price'] = Decimal(str(content[buy_key].get('Price', '0')))
                item[f'{buy_key}_Qty'] = Decimal(str(content[buy_key].get('Qty', '0')))
                item[f'{buy_key}_Sign'] = content[buy_key].get('Sign', 'N/A')
                item[f'{buy_key}_Time'] = content[buy_key].get('Time','N/A')

        # デバッグ用ログ
        logger.debug(f"Putting item to DynamoDB: {json.dumps(item, indent=2, default=str)}")

        await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: table.put_item(Item=item)
        )
        logger.info("データが正常に保存されました")
    except ClientError as e:
        logger.error(f"データ保存中にエラーが発生しました: {e}")
    except Exception as e:
        logger.error(f"予期せぬエラーが発生しました: {e}")
        logger.debug(f"受信したデータ: {json.dumps(content, indent=2)}")

async def on_message(ws, message):
    logger.info('メッセージを受信しました')
    try:
        content = json.loads(message)
        if not all(key in content for key in ['Symbol', 'CurrentPrice', 'CurrentPriceTime']):
            logger.warning('受信データに必要な情報が含まれていません')
            return
        await store_pushdata_in_dynamodb(content)
    except json.JSONDecodeError:
        logger.error('JSONのデコードに失敗しました')
    except Exception as e:
        logger.error(f'予期せぬエラーが発生しました: {e}')

def on_error(ws, error):
    logger.error(f'WebSocketエラー: {error}')

def on_close(ws, close_status_code, close_msg):
    logger.info('WebSocket接続が閉じられました')

def on_open(ws):
    logger.info('WebSocket接続が開かれました')

def run_websocket(loop):
    url = 'ws://localhost:18080/kabusapi/websocket'
    while True:
        try:
            ws = websocket.WebSocketApp(url,
                                        on_message=lambda ws, msg: asyncio.run_coroutine_threadsafe(on_message(ws, msg), loop),
                                        on_error=on_error,
                                        on_close=on_close,
                                        on_open=on_open)
            ws.run_forever()
        except Exception as e:
            logger.error(f'WebSocket接続エラー: {e}')
        logger.info('5秒後に再接続を試みます...')
        time.sleep(5)

async def main():
    loop = asyncio.get_event_loop()
    websocket_thread = threading.Thread(target=run_websocket, args=(loop,))
    websocket_thread.start()

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info('プログラムを終了します')

if __name__ == '__main__':
    asyncio.run(main())
