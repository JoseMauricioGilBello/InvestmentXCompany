import json
import requests
import boto3
import pandas as pd
import time
from decimal import *
from datetime import datetime , timedelta
    
def lambda_handler(event, context):
    # api_key save in a save place later
    API_KEY = 'wuvcWuN23lE2Z7hNMiGbRVd0loFsBkB3'
    # URL to get all symbols available on Polygon API
    GET_ALL_SYMBOLS_URL = 'https://api.polygon.io/v3/reference/tickers?sort=ticker&limit={}&apiKey={}'
    # URL to get the price of a symbol
    SYMBOLS_PRICE_URL = 'https://api.polygon.io/v2/aggs/ticker/{}/range/1/day/{}/{}?adjusted=true&sort=asc&apiKey={}'
                         
    
        
    # Get the list of all supported tickers from Polygon.io and load the results in a dynamo table call AvailableSymbols
    def get_tickers(url = GET_ALL_SYMBOLS_URL):
        # Limit the size of the response default is 100 and max is 1000.
        limit = event["queryStringParameters"]["limit"]
        alltickers = []
        session = requests.Session()
        #Initial request to get the tickers
        r = session.get(url.format(limit, API_KEY))
        data = r.json()
        #test without call the API Polygon commented
        '''
        f = open("src/result.json","r")
        #data = f.read()
        #data = json.loads(data)
        '''
        for i in range (len(data["results"])):
            alltickers.append(data["results"][i]["ticker"])
            #print(data["results"][i]["ticker"])

        return alltickers
    
    # Configure a list of symbols into table InterestedSymbols table. Then load prices information about those symbols in ... table.
    def configandloadprices():
        # Configuring list (load into )
        list1 = event["multiValueQueryStringParameters"]["Symbollist"]
        list2 = ''.join(list1)
        client_dynamo = boto3.resource('dynamodb')
        #The AvailableSymbols table has 2 fields. Default InterestedSymbols 1 (true) and list (list of symbols what interests the customer)
        table = client_dynamo.Table('AvailableSymbols')
        try:
            #response=table.put_item(Item={'InterestedSymbols': 1, 'list': list(list1.split(","))})
            response=table.put_item(Item={'InterestedSymbols': 1, 'list': list2.split(",")})
        except:
            raise
    
    # load configured stock
    def loadconfiguredsymbols():
        client_dynamo = boto3.resource('dynamodb')
        table = client_dynamo.Table('AvailableSymbols')
        response = table.get_item(Key={"InterestedSymbols":1})
        configuredstocks = response["Item"]
        #will return results in ascending order (oldest at the top)
        configuredstocks = configuredstocks["list"]
        return configuredstocks
    
    def getprice (symbol):
        startdate = event["queryStringParameters"]["startdate"]
        enddate = event["queryStringParameters"]["enddate"]
        session = requests.Session()
        r = session.get(SYMBOLS_PRICE_URL.format(symbol,startdate, enddate, API_KEY))
        info = r.json()
        # check if there are information, if there are create a dataframe in pandas 
        try:
            if info['queryCount'] >= 1:
                df = pd.DataFrame(info['results'])
                df['searchdate'] = pd.to_datetime("today")
                df['searchdate'] = df['searchdate'].astype(str)
                df['date'] = pd.to_datetime(df['t'], unit='ms')
                df['date'] =  df['date'].astype(str)
                #df['date'] =  df['date'].dt.date.astype(str)
                df['symbol'] = symbol
                df.rename(columns={'o': 'opening price', 'c': 'closing price', 'h': 'highest price', 'l': 'lowest price'}, inplace=True)
                df.drop(columns=['vw', 't', 'v', 'n'], inplace=True)
                stockpricejson = json.loads(df.to_json(orient="records"), parse_float=Decimal)
                return stockpricejson
            else:
                return 'No data found' 
        except KeyError:
            return 'No data found'        
    
    def loaddynamoSymbolsPrices(prices):
        client_dynamo = boto3.resource('dynamodb')
        # SymbolsPrices table save data of interested symbol
        table = client_dynamo.Table('SymbolsPrices')
        try:
            for item in prices:
                table.put_item(Item=item)
                #response=table.put_item(Item=prices)
                print('loaded')
        except:
            raise
        return 'data loaded'
    
    def createfilefromdynamo ():
        BUCKET_NAME = 'investmenxbucket'
        TEMP_FILENAME = '/tmp/symbolsinformation.csv'
        OUTPUT_KEY = 'symbolsinformation.csv'
        s3_resource = boto3.resource('s3')
        client_dynamo = boto3.resource('dynamodb')
        client_dynamo = boto3.resource('dynamodb')
        table = client_dynamo.Table('SymbolsPrices')
        response = table.scan()
        df = pd.DataFrame(response['Items'])
        df.to_csv(TEMP_FILENAME, index=False, header=True)
        # Upload temp file to S3
        s3_resource.Bucket(BUCKET_NAME).upload_file(TEMP_FILENAME, OUTPUT_KEY)

        return {
            'statusCode': 200,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Credentials": True,
                "content-type": "application/json"
            },
            'body': json.dumps('OK')
        }
    
    
        
    
    if event["queryStringParameters"]["service"] == "getsymbols":
        service = "get available symbols"
        result = get_tickers()
        result1 = ','.join(result)
        client_dynamo = boto3.resource('dynamodb')
        #The AvailableSymbols table has 2 fields. Default InterestedSymbols 1 (true) and list (list of symbols what interests the customer)
        table = client_dynamo.Table('AvailableSymbols')
        try:
            response=table.put_item(Item={'InterestedSymbols': 1, 'list': list(result1.split(","))})
            print('loaded')
        except:
            raise
        return {
       'statusCode': 200,
       'body':json.dumps({'service': service,'Available symbols': result})
        }    
 
    if event["queryStringParameters"]["service"] == "configandloadprices":
        service = 'Configure symbols and load prices for configuration of symbols and loading of prices for later analysis'
        result = configandloadprices()
        result2 = loadconfiguredsymbols()
        loadedsymbols = []
        Noloadedsymbols = []
        for i in result2:
            result3 = getprice(i)
            #loading to dynamo
            if result3 != 'No data found':
                loaddynamoSymbolsPrices(result3)
                loadedsymbols.append(i)
                time.sleep(3) # Sleep for 3 seconds
            else:
                Noloadedsymbols.append(i)
                time.sleep(3) # Sleep for 3 seconds
        return {
            'statusCode': 200,
            'body':json.dumps({'service': service,'Available symbols': result2,'symbols loaded to dynamodb' : loadedsymbols, 'no data for specified timeframe': Noloadedsymbols})
        }
    
    if event["queryStringParameters"]["service"] == "getspecificprice":
        service = 'Get specific price of a symbol'
        symbol = event["queryStringParameters"]["symbol"]
        prices = getprice(symbol)
        return {
            'statusCode': 200,
            'body':json.dumps({'service': service,'Chosen Symbol': symbol ,'The prices available in the time slot you chose are': prices}, default=str)
            }
    
    if event["queryStringParameters"]["service"] == "downloadcsv":
        service = 'download symbol information in csv'
        createfilefromdynamo()
        BUCKET_NAME = 'investmenxbucket'
        OUTPUT_KEY = 'symbolsinformation.csv'
        s3 = boto3.client("s3")
        fileObj = s3.get_object(Bucket = BUCKET_NAME, Key = OUTPUT_KEY)
        filecontent = fileObj["Body"].read()
        return {
            'statusCode': 200,
            'headers':{
                'Content-Type':'csv',
                'Content-disposition': 'attachment; filename={}'.format(OUTPUT_KEY)
            },
            'body': filecontent
            }