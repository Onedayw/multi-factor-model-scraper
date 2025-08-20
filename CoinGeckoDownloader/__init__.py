import azure.functions as func
import logging
import requests
import json
from datetime import datetime, timezone
import os


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('CoinGecko price chart downloader function started.')
    
    coins = ['bitcoin', 'ethereum', 'ripple', 'tether', 'binancecoin']
    base_url = "https://www.coingecko.com/price_charts/export/{}/usd.csv"
    
    results = {}
    
    for coin in coins:
        try:
            url = base_url.format(coin)
            logging.info(f'Downloading data for {coin} from {url}')
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            csv_data = response.text
            
            results[coin] = {
                'status': 'success',
                'data_size': len(csv_data),
                'url': url,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
            filename = f"{coin}_price_data_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
            
            container_name = os.environ.get('AZURE_STORAGE_CONTAINER', 'coingecko-data')
            azure_storage_connection = os.environ.get('AzureWebJobsStorage')
            
            if azure_storage_connection:
                try:
                    from azure.storage.blob import BlobServiceClient
                    blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection)
                    blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
                    blob_client.upload_blob(csv_data, overwrite=True)
                    results[coin]['storage_location'] = f"{container_name}/{filename}"
                    logging.info(f'Successfully uploaded {coin} data to blob storage')
                except Exception as e:
                    logging.error(f'Failed to upload {coin} data to blob storage: {str(e)}')
                    results[coin]['storage_error'] = str(e)
            else:
                logging.warning('Azure Storage connection not configured, data not persisted')
                
        except requests.exceptions.RequestException as e:
            logging.error(f'Failed to download data for {coin}: {str(e)}')
            results[coin] = {
                'status': 'error',
                'error': str(e),
                'url': base_url.format(coin),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        except Exception as e:
            logging.error(f'Unexpected error for {coin}: {str(e)}')
            results[coin] = {
                'status': 'error',
                'error': str(e),
                'url': base_url.format(coin),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
    
    success_count = sum(1 for result in results.values() if result['status'] == 'success')
    total_count = len(results)
    
    response_data = {
        'summary': {
            'total_coins': total_count,
            'successful_downloads': success_count,
            'failed_downloads': total_count - success_count,
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