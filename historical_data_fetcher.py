import pandas as pd
import yfinance as yf
import os
import asyncio
import aiohttp
from datetime import datetime, timedelta
import logging
from tqdm import tqdm
import numpy as np
from typing import List, Dict, Optional
import asyncpg
import time
import signal
import sys

# ======================== CONFIGURATION ======================== #
class HistoricalDataConfig:
    # Database Configuration (same as main code)
    POSTGRES_HOST = "localhost"
    POSTGRES_PORT = 5432
    POSTGRES_DB = "stock_scanner"
    POSTGRES_USER = "hodumaru"
    POSTGRES_PASSWORD = "Leetkd214"
    
    # Data Configuration
    START_DATE = "2012-01-01"
    END_DATE = "2024-12-31"
    EXCHANGE = "XNAS"
    DATA_DIR = "historical_data"
    
    # Download Configuration
    BATCH_SIZE = 50
    MAX_CONCURRENT_REQUESTS = 5
    RATE_LIMIT_DELAY = 0.2

# Initialize config
config = HistoricalDataConfig()

# ======================== LOGGING ======================== #
def setup_logging():
    """Setup logging for historical data fetcher"""
    logger = logging.getLogger('HistoricalDataFetcher')
    logger.setLevel(logging.INFO)
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Create formatter without emojis to avoid encoding issues
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # File handler with UTF-8 encoding
    file_handler = logging.FileHandler('historical_data_fetcher.log', encoding='utf-8')
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

# ======================== DATABASE MANAGER ======================== #
class HistoricalDataDBManager:
    """Database manager to fetch tickers from your main database"""
    
    def __init__(self):
        self.pool = None
    
    async def initialize(self):
        """Initialize database connection"""
        try:
            self.pool = await asyncpg.create_pool(
                host=config.POSTGRES_HOST,
                port=config.POSTGRES_PORT,
                database=config.POSTGRES_DB,
                user=config.POSTGRES_USER,
                password=config.POSTGRES_PASSWORD,
                min_size=1,
                max_size=10
            )
            logger.info("Database connection established")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    async def get_all_active_tickers(self) -> List[Dict]:
        """Get all active tickers from your main database - same method as main code"""
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT ticker, name, primary_exchange 
                    FROM tickers 
                    WHERE active = 1 AND primary_exchange = $1
                    ORDER BY ticker
                """, config.EXCHANGE)
                
                tickers = []
                for row in rows:
                    tickers.append({
                        'ticker': row['ticker'],
                        'name': row['name'],
                        'primary_exchange': row['primary_exchange']
                    })
                
                logger.info(f"Found {len(tickers)} active tickers for {config.EXCHANGE}")
                return tickers
                
        except Exception as e:
            logger.error(f"Error fetching tickers: {e}")
            return []
    
    async def close(self):
        """Close database connection"""
        if self.pool:
            await self.pool.close()

# ======================== ROBUST HISTORICAL DATA FETCHER ======================== #
class RobustHistoricalDataFetcher:
    """More robust historical data fetcher with better error handling and progress tracking"""
    
    def __init__(self):
        self.db_manager = HistoricalDataDBManager()
        self.data_dir = config.DATA_DIR
        self.shutdown_requested = False
        self.completed_tickers = set()
        self.failed_tickers = []
        
        # Create data directory
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Load progress if exists
        self._load_progress()
    
    def _load_progress(self):
        """Load progress from previous run"""
        progress_file = os.path.join(self.data_dir, 'progress.json')
        if os.path.exists(progress_file):
            try:
                import json
                with open(progress_file, 'r') as f:
                    progress = json.load(f)
                self.completed_tickers = set(progress.get('completed_tickers', []))
                self.failed_tickers = progress.get('failed_tickers', [])
                logger.info(f"Resumed progress: {len(self.completed_tickers)} completed, {len(self.failed_tickers)} failed")
            except Exception as e:
                logger.warning(f"Could not load progress: {e}")
    
    def _save_progress(self):
        """Save current progress"""
        try:
            import json
            progress_file = os.path.join(self.data_dir, 'progress.json')
            progress = {
                'completed_tickers': list(self.completed_tickers),
                'failed_tickers': self.failed_tickers,
                'last_updated': datetime.now().isoformat()
            }
            with open(progress_file, 'w') as f:
                json.dump(progress, f, indent=2)
        except Exception as e:
            logger.warning(f"Could not save progress: {e}")
    
    async def initialize(self):
        """Initialize the fetcher"""
        return await self.db_manager.initialize()
    
    async def fetch_all_historical_data(self):
        """Main method to fetch all historical data with robust error handling"""
        logger.info("Starting historical data fetch...")
        
        # Get all active tickers
        tickers = await self.db_manager.get_all_active_tickers()
        if not tickers:
            logger.error("No tickers found to fetch")
            return
        
        # Filter out already completed tickers
        remaining_tickers = [t for t in tickers if t['ticker'] not in self.completed_tickers]
        
        if not remaining_tickers:
            logger.info("All tickers already completed from previous run")
            return
        
        logger.info(f"Fetching historical data for {len(remaining_tickers)} remaining tickers from {config.START_DATE} to {config.END_DATE}")
        
        # Process in smaller batches for better stability
        total_successful = 0
        
        for i in range(0, len(remaining_tickers), config.BATCH_SIZE):
            if self.shutdown_requested:
                logger.info("Shutdown requested, stopping...")
                break
                
            batch = remaining_tickers[i:i + config.BATCH_SIZE]
            batch_successful = await self._process_batch_safely(batch, i, len(remaining_tickers))
            total_successful += batch_successful
            
            # Save progress after each batch
            self._save_progress()
            
            # Longer delay between batches
            if not self.shutdown_requested and i + config.BATCH_SIZE < len(remaining_tickers):
                logger.info(f"Batch completed. Waiting 2 seconds before next batch...")
                await asyncio.sleep(2)
        
        # Generate summary
        await self._generate_summary(total_successful)
        
        logger.info("Historical data fetch completed!")
    
    async def _process_batch_safely(self, batch: List[Dict], start_index: int, total_count: int) -> int:
        """Process a batch with comprehensive error handling"""
        batch_num = (start_index // config.BATCH_SIZE) + 1
        total_batches = (total_count + config.BATCH_SIZE - 1) // config.BATCH_SIZE
        
        logger.info(f"Processing batch {batch_num}/{total_batches} with {len(batch)} tickers")
        
        successful = 0
        
        for ticker_data in batch:
            if self.shutdown_requested:
                break
                
            ticker = ticker_data['ticker']
            
            # Skip if already completed (shouldn't happen due to filtering, but just in case)
            if ticker in self.completed_tickers:
                continue
            
            try:
                result = await self._fetch_single_ticker_safe(ticker_data)
                
                if result:
                    successful += 1
                    self.completed_tickers.add(ticker)
                else:
                    self.failed_tickers.append({
                        'ticker': ticker,
                        'error': 'Download or processing failed',
                        'timestamp': datetime.now().isoformat()
                    })
                
                # Small delay between tickers in batch
                await asyncio.sleep(config.RATE_LIMIT_DELAY)
                
            except asyncio.CancelledError:
                logger.info("Batch processing cancelled")
                raise
            except Exception as e:
                logger.error(f"Unexpected error processing {ticker}: {e}")
                self.failed_tickers.append({
                    'ticker': ticker,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                })
                continue
        
        logger.info(f"Batch {batch_num} complete: {successful} successful, {len(batch) - successful} failed")
        return successful
    
    async def _fetch_single_ticker_safe(self, ticker_data: Dict) -> bool:
        """Safely fetch data for a single ticker with comprehensive error handling"""
        ticker = ticker_data['ticker']
        
        try:
            logger.debug(f"Downloading {ticker}...")
            
            # Use a timeout for the download
            try:
                df = await asyncio.wait_for(
                    self._download_ticker(ticker),
                    timeout=30.0  # 30 second timeout
                )
            except asyncio.TimeoutError:
                logger.warning(f"Timeout downloading {ticker}")
                return False
            
            if df.empty:
                logger.warning(f"No data found for {ticker}")
                return False
            
            # Process the data
            df_processed = self._process_dataframe(df, ticker, ticker_data['name'])
            
            if df_processed.empty:
                logger.warning(f"No valid data after processing for {ticker}")
                return False
            
            # Save the data
            await self._save_ticker_data(df_processed, ticker)
            
            logger.debug(f"Successfully processed {ticker} ({len(df)} records)")
            return True
            
        except Exception as e:
            logger.error(f"Error processing {ticker}: {e}")
            return False
    
    async def _download_ticker(self, ticker: str) -> pd.DataFrame:
        """Download ticker data in thread pool"""
        loop = asyncio.get_event_loop()
        
        def download():
            try:
                # Add more parameters for better compatibility
                data = yf.download(
                    ticker,
                    start=config.START_DATE,
                    end=config.END_DATE,
                    progress=False,
                    auto_adjust=False,
                    threads=False,
                    # Additional parameters for better data handling
                    group_by='ticker',
                    actions=False
                )
                
                # Handle case where yfinance returns empty data
                if data.empty:
                    return pd.DataFrame()
                
                return data
                
            except Exception as e:
                logger.debug(f"yfinance error for {ticker}: {e}")
                return pd.DataFrame()
        
        return await loop.run_in_executor(None, download)
    
    def _process_dataframe(self, df: pd.DataFrame, ticker: str, name: str) -> pd.DataFrame:
        """Process and clean the dataframe with better error handling"""
        try:
            # Handle MultiIndex columns (common in yfinance)
            if isinstance(df.columns, pd.MultiIndex):
                # Flatten MultiIndex columns
                df.columns = ['_'.join(col).strip() for col in df.columns.values]
            
            # Reset index to get Date as column if it's in the index
            if not isinstance(df.index, pd.RangeIndex):
                df = df.reset_index()
            
            # Debug: Log the columns we received
            logger.debug(f"Columns for {ticker}: {list(df.columns)}")
            
            # More robust column mapping
            column_mapping = {}
            date_col = None
            
            for col in df.columns:
                col_str = str(col).lower()
                
                # Handle date column
                if 'date' in col_str:
                    column_mapping[col] = 'Date'
                    date_col = col
                # Handle price columns
                elif 'open' in col_str:
                    column_mapping[col] = 'Open'
                elif 'high' in col_str:
                    column_mapping[col] = 'High'
                elif 'low' in col_str:
                    column_mapping[col] = 'Low'
                elif 'close' in col_str and 'adj' not in col_str:
                    column_mapping[col] = 'Close'
                elif 'adj close' in col_str or 'adj_close' in col_str:
                    column_mapping[col] = 'Adj_Close'
                elif 'volume' in col_str:
                    column_mapping[col] = 'Volume'
            
            # Apply column mapping
            df = df.rename(columns=column_mapping)
            
            # Check required columns
            required_cols = ['Date', 'Open', 'High', 'Low', 'Close']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.warning(f"Missing required columns for {ticker}: {missing_cols}")
                return pd.DataFrame()
            
            # Select and reorder columns
            available_cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj_Close', 'Volume']
            cols_to_keep = [col for col in available_cols if col in df.columns]
            df = df[cols_to_keep].copy()
            
            # Add metadata
            df['Ticker'] = ticker
            df['Name'] = name if name else ticker
            df['Exchange'] = config.EXCHANGE
            
            # Convert data types with better error handling
            try:
                df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            except Exception as e:
                logger.warning(f"Date conversion error for {ticker}: {e}")
                return pd.DataFrame()
            
            # Convert numeric columns safely
            for col in ['Open', 'High', 'Low', 'Close', 'Adj_Close']:
                if col in df.columns:
                    try:
                        # Ensure we're working with a Series before conversion
                        if isinstance(df[col], (pd.Series, pd.DataFrame)):
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                        else:
                            logger.warning(f"Column {col} for {ticker} is not a Series, skipping")
                    except Exception as e:
                        logger.warning(f"Error converting {col} for {ticker}: {e}")
                        df[col] = np.nan
            
            if 'Volume' in df.columns:
                try:
                    df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
                    # Convert to int, handling NaN values
                    df['Volume'] = df['Volume'].replace([np.inf, -np.inf], 0).fillna(0).astype(int)
                except Exception as e:
                    logger.warning(f"Volume conversion error for {ticker}: {e}")
                    df['Volume'] = 0
            
            # Remove rows with critical missing data
            df = df.dropna(subset=['Date', 'Open', 'High', 'Low', 'Close'])
            
            if df.empty:
                return pd.DataFrame()
            
            # Sort and reset index
            df = df.sort_values('Date').reset_index(drop=True)
            
            return df
            
        except Exception as e:
            logger.error(f"Data processing error for {ticker}: {e}")
            return pd.DataFrame()
    
    async def _save_ticker_data(self, df: pd.DataFrame, ticker: str):
        """Save ticker data to CSV"""
        try:
            safe_ticker = "".join(c for c in ticker if c.isalnum() or c in ('-', '_')).rstrip()
            filename = f"{safe_ticker}.csv"
            filepath = os.path.join(self.data_dir, filename)
            df.to_csv(filepath, index=False)
        except Exception as e:
            logger.error(f"Error saving data for {ticker}: {e}")
            raise
    
    async def _generate_summary(self, successful: int):
        """Generate summary of the download process"""
        summary = {
            'download_date': datetime.now().isoformat(),
            'total_tickers_attempted': len(self.completed_tickers) + len(self.failed_tickers),
            'successful_downloads': len(self.completed_tickers),
            'failed_downloads': len(self.failed_tickers),
            'date_range': {
                'start': config.START_DATE,
                'end': config.END_DATE
            },
            'exchange': config.EXCHANGE,
            'failed_tickers': self.failed_tickers
        }
        
        # Save summary to JSON
        import json
        summary_file = os.path.join(self.data_dir, 'download_summary.json')
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Download Summary:")
        logger.info(f"  Successful: {len(self.completed_tickers)}")
        logger.info(f"  Failed: {len(self.failed_tickers)}")
        logger.info(f"  Data saved to: {self.data_dir}")
    
    async def create_combined_dataset(self):
        """Create combined dataset from downloaded files"""
        logger.info("Creating combined dataset...")
        
        try:
            all_data = []
            csv_files = [f for f in os.listdir(self.data_dir) if f.endswith('.csv') and f not in ['download_summary.csv', 'combined_dataset.csv']]
            
            for csv_file in tqdm(csv_files, desc="Combining data"):
                if self.shutdown_requested:
                    break
                    
                filepath = os.path.join(self.data_dir, csv_file)
                try:
                    df = pd.read_csv(filepath)
                    all_data.append(df)
                except Exception as e:
                    logger.warning(f"Error reading {csv_file}: {e}")
                    continue
            
            if all_data and not self.shutdown_requested:
                combined_df = pd.concat(all_data, ignore_index=True)
                combined_df['Date'] = pd.to_datetime(combined_df['Date'])
                combined_df = combined_df.sort_values(['Date', 'Ticker'])
                
                combined_file = os.path.join(self.data_dir, 'combined_dataset.csv')
                combined_df.to_csv(combined_file, index=False)
                
                logger.info(f"Combined dataset created: {len(combined_df):,} records, {len(csv_files)} tickers")
                
                # Create metadata file
                metadata = combined_df[['Ticker', 'Name', 'Exchange']].drop_duplicates()
                metadata_file = os.path.join(self.data_dir, 'ticker_metadata.csv')
                metadata.to_csv(metadata_file, index=False)
                
                return combined_file
            else:
                logger.warning("No data found to combine")
                return None
                
        except Exception as e:
            logger.error(f"Error creating combined dataset: {e}")
            return None
    
    async def close(self):
        """Close resources"""
        await self.db_manager.close()

# ======================== SIGNAL HANDLING ======================== #
def setup_signal_handling(fetcher):
    """Setup signal handling for graceful shutdown"""
    def signal_handler(signum, frame):
        logger.info("Received shutdown signal, stopping gracefully...")
        fetcher.shutdown_requested = True
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

# ======================== MAIN EXECUTION ======================== #
async def main():
    """Main function with comprehensive error handling"""
    fetcher = RobustHistoricalDataFetcher()
    
    # Setup signal handling for graceful shutdown
    setup_signal_handling(fetcher)
    
    try:
        # Initialize
        if not await fetcher.initialize():
            logger.error("Failed to initialize historical data fetcher")
            return
        
        logger.info("Starting historical data download...")
        logger.info("Press Ctrl+C to stop gracefully and save progress")
        
        # Fetch data
        await fetcher.fetch_all_historical_data()
        
        # Only create combined dataset if not shutdown
        if not fetcher.shutdown_requested:
            await fetcher.create_combined_dataset()
            logger.info("Historical data processing completed successfully!")
        else:
            logger.info("Processing stopped by user. Progress has been saved.")
        
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
    finally:
        await fetcher.close()
        logger.info("Fetcher closed successfully")

if __name__ == "__main__":
    # Set better event loop policy for Windows
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    # Run the main function
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")