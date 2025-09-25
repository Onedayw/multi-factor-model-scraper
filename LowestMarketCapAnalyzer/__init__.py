import azure.functions as func
import logging
import requests
import json
import pandas as pd
import io
from datetime import datetime, timezone
import os


def get_coin_list():
    """Fetch coin list from CoinGecko API"""
    try:
        coin_list_url = "https://api.coingecko.com/api/v3/coins/list"
        response = requests.get(coin_list_url, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f'Failed to fetch coin list: {str(e)}')
        return None


def analyze_lowest_market_caps(blob_service_client, container_name, start_date='2023-01-01'):
    """Download and analyze CSV files from blob storage to find lowest market caps since start_date"""
    lowest_caps = {}
    
    # Get coin list and mapping
    coin_list = get_coin_list()
    if not coin_list:
        logging.error('Failed to fetch coin list, falling back to default coins')
        coins = ['bitcoin', 'ethereum', 'ripple', 'tether', 'binancecoin']
        coin_mapping = {coin: {'symbol': coin, 'name': coin.title()} for coin in coins}
    else:
        coins = [coin['id'] for coin in coin_list]
        coin_mapping = {coin['id']: {'symbol': coin['symbol'], 'name': coin['name']} for coin in coin_list}
        logging.info(f'Will analyze data for {len(coins)} coins')
    
    # List blobs in container to find latest CSV files for each coin
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blobs = list(container_client.list_blobs())
        
        # Group blobs by coin and get the most recent for each
        coin_files = {}
        for blob in blobs:
            for coin in coins:
                if blob.name.startswith(f"market_cap/raw_data/{coin}_price_data_") and blob.name.endswith('.csv'):
                    if coin not in coin_files or blob.last_modified > coin_files[coin]['last_modified']:
                        coin_files[coin] = {'name': blob.name, 'last_modified': blob.last_modified}
        
        logging.info(f"Found CSV files for coins: {list(coin_files.keys())}")
        
        # Process each coin's data
        for coin, file_info in coin_files.items():
            try:
                # Download CSV data
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_info['name'])
                csv_data = blob_client.download_blob().readall().decode('utf-8')
                
                # Parse CSV data
                df = pd.read_csv(io.StringIO(csv_data))
                logging.info(f"Processing {coin}: {len(df)} rows, columns: {list(df.columns)}")
                
                # Handle different CSV formats from CoinGecko
                if 'snapped_at' in df.columns and 'price' in df.columns:
                    # CoinGecko format: snapped_at, price, market_cap, total_volume
                    df['date'] = pd.to_datetime(df['snapped_at'])
                    df_filtered = df[df['date'] >= start_date]
                    
                    if not df_filtered.empty:
                        if 'market_cap' in df.columns:
                            # Find lowest market cap
                            min_cap_row = df_filtered.loc[df_filtered['market_cap'].idxmin()]
                            lowest_caps[coin] = {
                                'coin_id': coin,
                                'coin_symbol': coin_mapping[coin]['symbol'].upper(),
                                'coin_name': coin_mapping[coin]['name'],
                                'lowest_market_cap': float(min_cap_row['market_cap']),
                                'date': min_cap_row['date'].strftime('%Y-%m-%d'),
                                'price_at_lowest': float(min_cap_row['price']),
                                'total_volume': float(min_cap_row.get('total_volume', 0)),
                                'current_price': float(df.iloc[-1]['price']) if not df.empty else None,
                                'current_market_cap': float(df.iloc[-1]['market_cap']) if 'market_cap' in df.columns and not df.empty else None
                            }
                        else:
                            # Fallback to lowest price if no market cap
                            min_price_row = df_filtered.loc[df_filtered['price'].idxmin()]
                            lowest_caps[coin] = {
                                'coin_id': coin,
                                'coin_symbol': coin_mapping[coin]['symbol'].upper(),
                                'coin_name': coin_mapping[coin]['name'],
                                'lowest_price': float(min_price_row['price']),
                                'date': min_price_row['date'].strftime('%Y-%m-%d'),
                                'market_cap': 'Not available'
                            }
                
                elif 'timestamp' in df.columns:
                    # Alternative timestamp format
                    df['date'] = pd.to_datetime(df['timestamp'])
                    df_filtered = df[df['date'] >= start_date]
                    
                    if 'market_cap' in df.columns and not df_filtered.empty:
                        min_cap_row = df_filtered.loc[df_filtered['market_cap'].idxmin()]
                        lowest_caps[coin] = {
                            'coin_id': coin,
                            'coin_symbol': coin_mapping[coin]['symbol'].upper(),
                            'coin_name': coin_mapping[coin]['name'],
                            'lowest_market_cap': float(min_cap_row['market_cap']),
                            'date': min_cap_row['date'].strftime('%Y-%m-%d'),
                            'price_at_lowest': float(min_cap_row.get('price', 0))
                        }
                
                logging.info(f"Successfully analyzed {coin}")
                        
            except Exception as e:
                logging.error(f"Error analyzing {coin} data: {str(e)}")
                continue
    
    except Exception as e:
        logging.error(f"Error accessing blob storage: {str(e)}")
        return {}
    
    return lowest_caps


def save_analysis_to_csv(lowest_caps, blob_service_client, container_name, overwrite=False):
    """Save lowest market caps analysis to CSV file in blob storage"""
    if not lowest_caps:
        return None
        
    try:
        # Create DataFrame from results
        df_results = pd.DataFrame.from_dict(lowest_caps, orient='index')
        
        # Sort by lowest market cap if available
        if 'lowest_market_cap' in df_results.columns:
            df_results = df_results.sort_values('lowest_market_cap')
        
        # Generate CSV content
        csv_content = df_results.to_csv(index=False)
        
        # Upload to blob storage
        filename = f"lowest_market_caps_analysis_{datetime.now(timezone.utc).strftime('%Y%m%d')}.csv"
        
        blob_path = f"market_cap/analysis/{filename}"
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
        blob_client.upload_blob(csv_content, overwrite=overwrite)
        
        logging.info(f'Analysis CSV saved: {blob_path}')
        return blob_path
        
    except Exception as e:
        logging.error(f"Failed to save analysis CSV: {str(e)}")
        return None


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('LowestMarketCapAnalyzer function started.')
    
    # Get parameters
    start_date = req.params.get('start_date', '2023-01-01')
    overwrite = req.params.get('overwrite', 'false').lower() == 'true'
    container_name = os.environ.get('AZURE_STORAGE_CONTAINER', 'coingecko-data')
    azure_storage_connection = os.environ.get('AzureWebJobsStorage')
    
    if not azure_storage_connection:
        error_msg = "Azure Storage connection not configured"
        logging.error(error_msg)
        return func.HttpResponse(
            json.dumps({"error": error_msg}),
            status_code=500,
            mimetype="application/json"
        )
    
    try:
        from azure.storage.blob import BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection)
        
        # Check if today's analysis file already exists (only skip if overwrite is False)
        if not overwrite:
            today_filename = f"lowest_market_caps_analysis_{datetime.now(timezone.utc).strftime('%Y%m%d')}.csv"
            today_blob_path = f"market_cap/analysis/{today_filename}"
            
            try:
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=today_blob_path)
                if blob_client.exists():
                    logging.info(f'Analysis file {today_blob_path} already exists, skipping analysis')
                    return func.HttpResponse(
                        json.dumps({
                            "message": "Analysis already completed for today",
                            "existing_file": today_blob_path,
                            "execution_timestamp": datetime.now(timezone.utc).isoformat()
                        }),
                        status_code=200,
                        mimetype="application/json"
                    )
            except Exception as e:
                logging.error(f'Error checking if analysis file exists: {str(e)}')
                # Continue with analysis if check fails
        
        logging.info(f'Starting analysis with start_date: {start_date}')
        
        # Analyze data
        lowest_caps = analyze_lowest_market_caps(blob_service_client, container_name, start_date)
        
        if not lowest_caps:
            return func.HttpResponse(
                json.dumps({
                    "message": "No data found for analysis",
                    "start_date": start_date,
                    "container": container_name
                }),
                status_code=404,
                mimetype="application/json"
            )
        
        # Save results
        analysis_filename = save_analysis_to_csv(lowest_caps, blob_service_client, container_name, overwrite)
        
        response_data = {
            "summary": {
                "coins_analyzed": len(lowest_caps),
                "start_date": start_date,
                "analysis_file": analysis_filename,
                "execution_timestamp": datetime.now(timezone.utc).isoformat()
            },
            "results": lowest_caps
        }
        
        logging.info(f'Analysis completed for {len(lowest_caps)} coins')
        
        return func.HttpResponse(
            json.dumps(response_data, indent=2),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        error_msg = f"Error during analysis: {str(e)}"
        logging.error(error_msg)
        return func.HttpResponse(
            json.dumps({"error": error_msg}),
            status_code=500,
            mimetype="application/json"
        )