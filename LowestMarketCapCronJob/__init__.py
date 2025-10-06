import azure.functions as func
import logging
import requests
import time
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


def call_coingecko_downloader(start_index, max_coins=100, overwrite=False):
    """Call the CoinGeckoDownloader function"""
    try:
        # Get function URL from environment variable
        downloader_url = os.environ.get('COINGECKO_DOWNLOADER_URL')
        if not downloader_url:
            # Fallback to constructing from base URL
            base_url = os.environ.get('AZURE_FUNCTIONS_BASE_URL')
            if not base_url:
                # Use default URL with warning
                base_url = 'market-cap-scraper-func-aabrdce2a7cxavax.canadacentral-01.azurewebsites.net'
                logging.warning('AZURE_FUNCTIONS_BASE_URL not set, using default URL. Set this environment variable for production.')
            downloader_url = f"{base_url}/api/CoinGeckoDownloader"

        # Get function key from environment variable
        function_code = os.environ.get('AZURE_FUNCTION_CODE')
        if not function_code:
            raise ValueError('AZURE_FUNCTION_CODE environment variable not set')

        params = {
            'code': function_code,
            'start_index': start_index,
            'max_coins': max_coins,
            'overwrite': str(overwrite).lower()
        }
        
        logging.info(f'Calling CoinGeckoDownloader with start_index={start_index}, max_coins={max_coins}')
        response = requests.get(downloader_url, params=params, timeout=300)  # 5 minute timeout
        response.raise_for_status()
        
        return response.json()
    except Exception as e:
        logging.error(f'Failed to call CoinGeckoDownloader: {str(e)}')
        return None


def call_lowest_market_cap_analyzer(start_date='2023-01-01', overwrite=False):
    """Call the LowestMarketCapAnalyzer function"""
    try:
        # Get function URL from environment variable
        analyzer_url = os.environ.get('MARKET_CAP_ANALYZER_URL')
        if not analyzer_url:
            # Fallback to constructing from base URL
            base_url = os.environ.get('AZURE_FUNCTIONS_BASE_URL')
            if not base_url:
                # Use default URL with warning
                base_url = 'market-cap-scraper-func-aabrdce2a7cxavax.canadacentral-01.azurewebsites.net'
                logging.warning('AZURE_FUNCTIONS_BASE_URL not set, using default URL. Set this environment variable for production.')
            analyzer_url = f"{base_url}/api/LowestMarketCapAnalyzer"

        # Get function key from environment variable
        function_code = os.environ.get('AZURE_FUNCTION_CODE')
        if not function_code:
            raise ValueError('AZURE_FUNCTION_CODE environment variable not set')

        params = {
            'code': function_code,
            'start_date': start_date,
            'overwrite': str(overwrite).lower()
        }
        
        logging.info(f'Calling LowestMarketCapAnalyzer with start_date={start_date}')
        response = requests.get(analyzer_url, params=params, timeout=300)  # 5 minute timeout
        response.raise_for_status()
        
        return response.json()
    except Exception as e:
        logging.error(f'Failed to call LowestMarketCapAnalyzer: {str(e)}')
        return None


def main(mytimer: func.TimerRequest) -> None:
    logging.info('LowestMarketCapCronJob timer function started.')
    
    start_time = datetime.now(timezone.utc)
    
    if mytimer.past_due:
        logging.info('The timer is past due!')
    
    # Configuration - can be set via environment variables
    overwrite = os.environ.get('CRONJOB_OVERWRITE', 'false').lower() == 'true'
    start_date = os.environ.get('CRONJOB_START_DATE', '2023-01-01')
    max_coins_per_batch = int(os.environ.get('CRONJOB_MAX_COINS_PER_BATCH', '200'))
    delay_between_calls = int(os.environ.get('CRONJOB_DELAY_SECONDS', '1'))
    
    # Get total number of coins available
    coin_list = get_coin_list()
    if not coin_list:
        logging.error('Failed to fetch coin list - aborting cron job')
        return
    
    total_coins = len(coin_list)
    logging.info(f'Total coins available: {total_coins}')
    
    # Calculate number of batches needed
    num_batches = (total_coins + max_coins_per_batch - 1) // max_coins_per_batch
    logging.info(f'Will process {num_batches} batches of up to {max_coins_per_batch} coins each')
    
    # Track download results
    total_successful = 0
    total_failed = 0
    
    # Process all coins in batches
    for batch_num in range(num_batches):
        start_index = batch_num * max_coins_per_batch
        
        logging.info(f'Processing batch {batch_num + 1}/{num_batches} (start_index={start_index})')
        
        # Call CoinGecko downloader
        download_result = call_coingecko_downloader(start_index, max_coins_per_batch, overwrite)
        
        if download_result:
            # Update counters
            summary = download_result.get('summary', {})
            batch_successful = summary.get('successful_downloads', 0)
            batch_failed = summary.get('failed_downloads', 0)
            total_successful += batch_successful
            total_failed += batch_failed
            
            logging.info(f'Batch {batch_num + 1} completed: {batch_successful} successful, {batch_failed} failed')
        else:
            logging.error(f'Batch {batch_num + 1} failed')
            total_failed += max_coins_per_batch  # Assume all failed if no response
        
        # Wait before next batch (except for the last batch)
        if batch_num < num_batches - 1:
            logging.info(f'Waiting {delay_between_calls} seconds before next batch...')
            time.sleep(delay_between_calls)
    
    download_end_time = datetime.now(timezone.utc)
    download_duration = (download_end_time - start_time).total_seconds()
    
    logging.info(f'Download phase completed in {download_duration:.2f} seconds')
    logging.info(f'Total successful downloads: {total_successful}, Total failed: {total_failed}')
    
    # Call analyzer after all downloads complete
    logging.info('Starting analysis phase...')
    analyzer_result = call_lowest_market_cap_analyzer(start_date, overwrite)
    
    analysis_end_time = datetime.now(timezone.utc)
    total_duration = (analysis_end_time - start_time).total_seconds()
    
    if analyzer_result:
        logging.info('Analysis completed successfully')
        analysis_summary = analyzer_result.get('summary', {})
        coins_analyzed = analysis_summary.get('coins_analyzed', 0)
        logging.info(f'Analyzed {coins_analyzed} coins')
    else:
        logging.error('Analysis failed')
    
    logging.info(f'LowestMarketCapCronJob completed in {total_duration:.2f} seconds')
    logging.info(f'Final summary: {total_successful} downloads successful, {total_failed} failed')
