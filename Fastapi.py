import os
import json
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import uvicorn
from typing import Optional, Dict, Any, List, Tuple

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)
try:
    table = dynamodb.Table(os.getenv('DYNAMODB_TABLE_NAME'))
    print(f"Table '{os.getenv('DYNAMODB_TABLE_NAME')}' initialized successfully")
except Exception as e:
    print(f"Error initializing DynamoDB table: {str(e)}")
    raise

async def get_all_data(symbol: str, date: str, last_evaluated_key: Optional[str] = None) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    query_params = {
        'KeyConditionExpression': Key('symbol').eq(symbol) & Key('get_time').begins_with(date),
    }

    all_data = []
    total_size = 0
    max_size = 50 * 1024 * 1024  # 50MB limit

    if last_evaluated_key:
        query_params['ExclusiveStartKey'] = json.loads(last_evaluated_key)

    while True:
        response = table.query(**query_params)
        items = response.get('Items', [])
        all_data.extend(items)

        # Estimate the size of the data
        total_size += sum(len(str(item)) for item in items)

        if 'LastEvaluatedKey' not in response or total_size >= max_size:
            break

        query_params['ExclusiveStartKey'] = response['LastEvaluatedKey']

    next_key = response.get('LastEvaluatedKey') if total_size >= max_size else None
    return all_data, next_key

@app.get("/api/price_volume_data/{symbol}")
async def get_price_volume_data(
        symbol: str,
        date: str,
        last_evaluated_key: Optional[str] = None
):
    try:
        print(f"Querying for symbol: {symbol}, date: {date}, last_evaluated_key: {last_evaluated_key}")

        all_data, next_key = await get_all_data(symbol, date, last_evaluated_key)

        data = [
            {
                'timestamp': item['get_time'],
                'price': float(item['current_price']),
                'volume': float(item['TradingVolume']),
                'VWAP': float(item['VWAP']),
                'HighPrice': float(item['HighPrice']),
                'LowPrice': float(item['LowPrice']),
            } for item in all_data
        ]

        result = {
            'data': data,
            'last_evaluated_key': json.dumps(next_key) if next_key else None
        }

        print(f"Processed price volume data count: {len(data)}")
        return result
    except ClientError as e:
        print(f"ClientError: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch price volume data: {str(e)}")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {str(e)}")

@app.get("/api/board_data/{symbol}")
async def get_board_data(
        symbol: str,
        date: str,
        last_evaluated_key: Optional[str] = None
):
    try:
        print(f"Querying board data for symbol: {symbol}, date: {date}, last_evaluated_key: {last_evaluated_key}")

        all_data, next_key = await get_all_data(symbol, date, last_evaluated_key)

        data = [
            {
                'timestamp': item['get_time'],
                'OverSellQty': float(item['OverSellQty']),
                'Sell1_Price': float(item['Sell1_Price']),
                'Sell1_Qty': float(item['Sell1_Qty']),
                'Sell2_Price': float(item['Sell2_Price']),
                'Sell2_Qty': float(item['Sell2_Qty']),
                'Sell3_Price': float(item['Sell3_Price']),
                'Sell3_Qty': float(item['Sell3_Qty']),
                'Sell4_Price': float(item['Sell4_Price']),
                'Sell4_Qty': float(item['Sell4_Qty']),
                'Sell5_Price': float(item['Sell5_Price']),
                'Sell5_Qty': float(item['Sell5_Qty']),
                'Sell6_Price': float(item['Sell6_Price']),
                'Sell6_Qty': float(item['Sell6_Qty']),
                'Sell7_Price': float(item['Sell7_Price']),
                'Sell7_Qty': float(item['Sell7_Qty']),
                'Sell8_Price': float(item['Sell8_Price']),
                'Sell8_Qty': float(item['Sell8_Qty']),
                'Sell9_Price': float(item['Sell9_Price']),
                'Sell9_Qty': float(item['Sell9_Qty']),
                'Sell10_Price': float(item['Sell10_Price']),
                'Sell10_Qty': float(item['Sell10_Qty']),
                'Buy1_Price': float(item['Buy1_Price']),
                'Buy1_Qty': float(item['Buy1_Qty']),
                'Buy2_Price': float(item['Buy2_Price']),
                'Buy2_Qty': float(item['Buy2_Qty']),
                'Buy3_Price': float(item['Buy3_Price']),
                'Buy3_Qty': float(item['Buy3_Qty']),
                'Buy4_Price': float(item['Buy4_Price']),
                'Buy4_Qty': float(item['Buy4_Qty']),
                'Buy5_Price': float(item['Buy5_Price']),
                'Buy5_Qty': float(item['Buy5_Qty']),
                'Buy6_Price': float(item['Buy6_Price']),
                'Buy6_Qty': float(item['Buy6_Qty']),
                'Buy7_Price': float(item['Buy7_Price']),
                'Buy7_Qty': float(item['Buy7_Qty']),
                'Buy8_Price': float(item['Buy8_Price']),
                'Buy8_Qty': float(item['Buy8_Qty']),
                'Buy9_Price': float(item['Buy9_Price']),
                'Buy9_Qty': float(item['Buy9_Qty']),
                'Buy10_Price': float(item['Buy10_Price']),
                'Buy10_Qty': float(item['Buy10_Qty']),
                'UnderBuyQty': float(item['UnderBuyQty']),
            } for item in all_data
        ]

        result = {
            'data': data,
            'last_evaluated_key': json.dumps(next_key) if next_key else None
        }

        print(f"Processed board data count: {len(data)}")
        return result
    except ClientError as e:
        print(f"ClientError in get_board_data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch board data: {str(e)}")
    except Exception as e:
        print(f"Unexpected error in get_board_data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred while fetching board data: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
