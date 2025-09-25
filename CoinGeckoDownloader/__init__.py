import azure.functions as func
import logging
import requests
import json
import asyncio
import aiohttp
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


async def download_coin_data(session, coin, coin_mapping, base_url, blob_service_client, container_name, overwrite):
    """Download data for a single coin asynchronously"""
    try:
        filename = f"{coin}_price_data_{datetime.now(timezone.utc).strftime('%Y%m%d')}.csv"
        blob_path = f"market_cap/raw_data/{filename}"
        
        # Check if file already exists
        if blob_service_client:
            try:
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
                
                # Check if blob exists (only skip if overwrite is False)
                if not overwrite and blob_client.exists():
                    logging.info(f'File {blob_path} already exists, skipping download for {coin}')
                    return coin, {
                        'status': 'skipped',
                        'reason': 'file_already_exists',
                        'existing_file': blob_path,
                        'symbol': coin_mapping[coin]['symbol'],
                        'name': coin_mapping[coin]['name'],
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    }
            except Exception as e:
                logging.error(f'Error checking if blob exists for {coin}: {str(e)}')
                # Continue with download if check fails
        
        url = base_url.format(coin)
        logging.info(f'Downloading data for {coin} from {url}')
        
        async with session.get(url, timeout=30) as response:
            response.raise_for_status()
            csv_data = await response.text()
            
            result = {
                'status': 'success',
                'data_size': len(csv_data),
                'url': url,
                'symbol': coin_mapping[coin]['symbol'],
                'name': coin_mapping[coin]['name'],
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            if blob_service_client:
                try:
                    blob_client.upload_blob(csv_data, overwrite=overwrite)
                    result['storage_location'] = f"{container_name}/{blob_path}"
                    logging.info(f'Successfully uploaded {coin} data to blob storage')
                except Exception as e:
                    logging.error(f'Failed to upload {coin} data to blob storage: {str(e)}')
                    result['storage_error'] = str(e)
            else:
                logging.warning('Azure Storage connection not configured, data not persisted')
                
            return coin, result
            
    except Exception as e:
        logging.error(f'Failed to download data for {coin}: {str(e)}')
        return coin, {
            'status': 'error',
            'error': str(e),
            'url': base_url.format(coin),
            'symbol': coin_mapping.get(coin, {}).get('symbol', 'unknown'),
            'name': coin_mapping.get(coin, {}).get('name', 'unknown'),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }


async def download_all_coins_async(coins, coin_mapping, base_url, blob_service_client, container_name, overwrite):
    """Download data for all coins concurrently"""
    results = {}
    
    # Use aiohttp for async HTTP requests
    connector = aiohttp.TCPConnector(limit=100, limit_per_host=10)  # Limit concurrent connections
    timeout = aiohttp.ClientTimeout(total=30)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Create tasks for all coin downloads
        tasks = []
        for coin in coins:
            task = download_coin_data(session, coin, coin_mapping, base_url, blob_service_client, container_name, overwrite)
            tasks.append(task)
        
        # Run downloads concurrently with batching to avoid overwhelming the API
        batch_size = 20  # Process 20 coins at a time to reduce timeout risk
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            logging.info(f'Processing batch {i//batch_size + 1} of {len(tasks)//batch_size + 1} ({len(batch)} coins)')
            
            batch_results = await asyncio.gather(*batch, return_exceptions=True)
            
            # Process batch results
            for result in batch_results:
                if isinstance(result, Exception):
                    logging.error(f'Batch download error: {str(result)}')
                else:
                    coin, coin_result = result
                    results[coin] = coin_result
            
            # Small delay between batches to be respectful to the API
            if i + batch_size < len(tasks):
                await asyncio.sleep(1)
    
    return results


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('CoinGecko price chart downloader function started.')
    
    # Get parameters
    overwrite = req.params.get('overwrite', 'false').lower() == 'true'
    max_coins = int(req.params.get('max_coins', '100'))  # Limit number of coins to process
    start_index = int(req.params.get('start_index', '0'))  # Allow processing in chunks
    
    # Fetch coin list from CoinGecko API
    coin_list = get_coin_list()
    if not coin_list:
        logging.error('Failed to fetch coin list, falling back to default coins')
        coins = ['bitcoin', 'ethereum', 'ripple', 'tether', 'binancecoin']
        coin_mapping = {coin: {'symbol': coin, 'name': coin.title()} for coin in coins}
    else:
        all_coins = [coin['id'] for coin in coin_list]
        coin_mapping = {coin['id']: {'symbol': coin['symbol'], 'name': coin['name']} for coin in coin_list}
        
        # Apply chunking to avoid timeout
        end_index = min(start_index + max_coins, len(all_coins))
        coins = all_coins[start_index:end_index]
        
        logging.info(f'Fetched {len(all_coins)} total coins from CoinGecko API')
        logging.info(f'Processing coins {start_index} to {end_index-1} ({len(coins)} coins)')
    
    base_url = "https://www.coingecko.com/price_charts/export/{}/usd.csv"
    
    container_name = os.environ.get('AZURE_STORAGE_CONTAINER', 'coingecko-data')
    azure_storage_connection = os.environ.get('AzureWebJobsStorage')
    
    # Set up blob service client if available
    blob_service_client = None
    if azure_storage_connection:
        try:
            from azure.storage.blob import BlobServiceClient
            blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection)
        except Exception as e:
            logging.error(f'Failed to create blob service client: {str(e)}')
    
    # Run async downloads with timeout
    try:
        # Set a timeout for the entire operation (4 minutes for Azure Functions)
        results = asyncio.wait_for(
            download_all_coins_async(coins, coin_mapping, base_url, blob_service_client, container_name, overwrite),
            timeout=240  # 4 minutes
        )
        results = asyncio.run(results)
    except asyncio.TimeoutError:
        logging.error('Download operation timed out after 4 minutes')
        return func.HttpResponse(
            json.dumps({
                "error": "Operation timed out. Try using smaller max_coins parameter or process in chunks.",
                "suggestion": "Use max_coins=50 or process in batches with start_index parameter"
            }),
            status_code=408,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f'Error running async downloads: {str(e)}')
        return func.HttpResponse(
            json.dumps({"error": f"Failed to run downloads: {str(e)}"}),
            status_code=500,
            mimetype="application/json"
        )
    
    success_count = sum(1 for result in results.values() if result['status'] in ['success', 'skipped'])
    total_count = len(results)
    
    response_data = {
        'summary': {
            'total_coins_processed': total_count,
            'successful_downloads': success_count,
            'failed_downloads': total_count - success_count,
            'start_index': start_index,
            'end_index': start_index + len(coins) - 1,
            'total_coins_available': len(coin_list) if coin_list else 5,
            'has_more': start_index + len(coins) < (len(coin_list) if coin_list else 5),
            'next_start_index': start_index + len(coins) if start_index + len(coins) < (len(coin_list) if coin_list else 5) else None,
            'execution_timestamp': datetime.now(timezone.utc).isoformat()
        },
        'results': results
    }
    
    logging.info(f'Completed processing. Success: {success_count}/{total_count}')
    
    return func.HttpResponse(
        json.dumps(response_data, indent=2),
        status_code=200,
        mimetype="application/json"
    )