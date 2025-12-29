import pandas as pd
import os
import json
import asyncio
import aiohttp
from typing import Dict, List, Optional
import logging
from tqdm import tqdm
import time

# ======================== CONFIGURATION ======================== #
class BacktestSectorConfig:
    # Directory paths
    HISTORICAL_TICKERS_DIR = r"C:\Users\kyung\StockScanner\historical_data\historical_tickers"
    BACKTEST_SECTORS_DIR = r"C:\Users\kyung\StockScanner\historical_data\backtest_sectors"
    JSON_FILES_DIR = r"C:\Users\kyung\StockScanner\json_files"
    
    # API Configuration
    POLYGON_API_KEY = "ld1Poa63U6t4Y2MwOCA2JeKQyHVrmyg8"
    
    # Sector Configuration
    SECTOR_MAPPING_FILE = os.path.join(JSON_FILES_DIR, "backtest_sector_mapping.json")
    SECTOR_CACHE_FILE = os.path.join(JSON_FILES_DIR, "backtest_sector_cache.json")
    
    # Rate limiting
    MAX_CONCURRENT_REQUESTS = 50
    RATE_LIMIT_DELAY = 0.1
    REQUEST_TIMEOUT = 30

# Initialize config
config = BacktestSectorConfig()

# ======================== LOGGING ======================== #
def setup_logging():
    """Configure logging for backtest sector scanner"""
    logger = logging.getLogger("BacktestSectorScanner")
    logger.setLevel(logging.INFO)
    
    # Create formatter without emojis for Windows compatibility
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler
    log_dir = os.path.join(os.path.dirname(__file__), "_logs")
    os.makedirs(log_dir, exist_ok=True)
    file_handler = logging.FileHandler(os.path.join(log_dir, "backtest_sector_scanner.log"), encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger

logger = setup_logging()

# ======================== SECTOR MAPPING MANAGER ======================== #
class BacktestSectorMappingManager:
    """Manager for sector mapping and grouping that loads from JSON file"""
    
    def __init__(self, mapping_file: str = None):
        self.mapping_file = mapping_file or config.SECTOR_MAPPING_FILE
        self.sector_mapping = {}
        self.major_sectors = []
        self._load_mapping()
        
    def _load_mapping(self):
        """Load sector mapping from JSON file"""
        try:
            if os.path.exists(self.mapping_file):
                with open(self.mapping_file, 'r', encoding='utf-8') as f:
                    loaded_mapping = json.load(f)
                
                self.sector_mapping = loaded_mapping.get('sector_mapping', {})
                self.major_sectors = loaded_mapping.get('major_sectors', [])
                
                logger.info(f"Loaded sector mapping from {self.mapping_file}")
                logger.info(f"   - {len(self.sector_mapping)} detailed sector mappings")
                logger.info(f"   - {len(self.major_sectors)} major sectors")
                    
            else:
                logger.error(f"Sector mapping file not found at {self.mapping_file}")
                # Create a default mapping file if it doesn't exist
                self._create_default_mapping()
                
        except Exception as e:
            logger.error(f"Failed to load sector mapping: {e}")
            # Create default mapping as fallback
            self._create_default_mapping()
    
    def _create_default_mapping(self):
        """Create a default sector mapping file"""
        try:
            default_mapping = {
                "sector_mapping": {
                    "Technology": "Technology",
                    "Information Technology": "Technology",
                    "Electronic Technology": "Technology",
                    "Software": "Technology",
                    "Health Care": "Health Care",
                    "Healthcare": "Health Care",
                    "Medical": "Health Care",
                    "Financials": "Financials",
                    "Finance": "Financials",
                    "Banking": "Financials",
                    "Consumer Discretionary": "Consumer Discretionary",
                    "Consumer Staples": "Consumer Staples",
                    "Industrials": "Industrials",
                    "Energy": "Energy",
                    "Utilities": "Utilities",
                    "Real Estate": "Real Estate",
                    "Materials": "Materials",
                    "Communication Services": "Communication Services",
                    "Telecommunications": "Communication Services"
                },
                "major_sectors": [
                    "Technology",
                    "Health Care", 
                    "Financials",
                    "Consumer Discretionary",
                    "Consumer Staples",
                    "Industrials",
                    "Energy",
                    "Utilities",
                    "Real Estate",
                    "Materials",
                    "Communication Services",
                    "Other"
                ]
            }
            
            # Save default mapping
            os.makedirs(os.path.dirname(self.mapping_file), exist_ok=True)
            with open(self.mapping_file, 'w', encoding='utf-8') as f:
                json.dump(default_mapping, f, indent=2, ensure_ascii=False)
            
            self.sector_mapping = default_mapping['sector_mapping']
            self.major_sectors = default_mapping['major_sectors']
            
            logger.info(f"Created default sector mapping file at {self.mapping_file}")
            
        except Exception as e:
            logger.error(f"Failed to create default mapping: {e}")
            # Set fallback mappings
            self.sector_mapping = {}
            self.major_sectors = ["Technology", "Health Care", "Financials", "Other"]
    
    def get_major_sector(self, detailed_sector: Optional[str]) -> str:
        """Map detailed sector to major sector using JSON mapping"""
        if not detailed_sector or detailed_sector == 'Unknown':
            return 'Other'
        
        # Exact match from JSON mapping
        if detailed_sector in self.sector_mapping:
            return self.sector_mapping[detailed_sector]
        
        # Case-insensitive partial match
        detailed_lower = detailed_sector.lower()
        for detailed, major in self.sector_mapping.items():
            if detailed.lower() in detailed_lower or detailed_lower in detailed.lower():
                return major
        
        # Enhanced keyword matching as fallback
        return self._keyword_fallback_mapping(detailed_lower)
    
    def _keyword_fallback_mapping(self, detailed_sector_lower: str) -> str:
        """Enhanced keyword-based fallback mapping"""
        keyword_mappings = [
            (['tech', 'software', 'computer', 'electronic', 'internet', 'semiconductor', 'data processing'], 'Technology'),
            (['health', 'medical', 'pharma', 'bio', 'drug', 'hospital', 'care', 'biotechnology'], 'Health Care'),
            (['bank', 'finance', 'insurance', 'invest', 'credit', 'lending', 'mortgage', 'financial'], 'Financials'),
            (['retail', 'consumer', 'auto', 'appliance', 'entertainment', 'travel', 'hotel', 'restaurant'], 'Consumer Discretionary'),
            (['food', 'beverage', 'grocery', 'staples', 'dairy', 'tobacco'], 'Consumer Staples'),
            (['industrial', 'manufactur', 'machine', 'engine', 'construction', 'engineering', 'aerospace'], 'Industrials'),
            (['oil', 'gas', 'energy', 'petroleum', 'drilling', 'crude', 'refining'], 'Energy'),
            (['electric', 'utility', 'water', 'power', 'sanitary', 'gas utility'], 'Utilities'),
            (['real estate', 'property', 'reit', 'building', 'apartment', 'realty'], 'Real Estate'),
            (['mining', 'metal', 'material', 'chemical', 'steel', 'plastic', 'rubber', 'paper'], 'Materials'),
            (['telecom', 'communication', 'media', 'broadcast', 'publishing', 'radio', 'tv', 'cable'], 'Communication Services')
        ]
        
        for keywords, sector in keyword_mappings:
            if any(keyword in detailed_sector_lower for keyword in keywords):
                return sector
        
        return 'Other'

# ======================== SECTOR DATA FETCHER ======================== #
class BacktestSectorDataFetcher:
    """Fetches sector data for historical tickers using similar logic to main code"""
    
    def __init__(self):
        self.api_key = config.POLYGON_API_KEY
        self.semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_REQUESTS)
        self.sector_cache = self._load_sector_cache()
        self.sector_mapping_manager = BacktestSectorMappingManager()
        
    def _load_sector_cache(self) -> Dict[str, str]:
        """Load sector cache from file"""
        try:
            if os.path.exists(config.SECTOR_CACHE_FILE):
                with open(config.SECTOR_CACHE_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception:
            pass
        return {}
    
    def _save_sector_cache(self):
        """Save sector cache to file"""
        try:
            with open(config.SECTOR_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.sector_cache, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Failed to save sector cache: {e}")
    
    async def get_ticker_sector(self, ticker: str) -> Optional[str]:
        """Get sector information for a ticker - similar to main code logic"""
        # Check cache first
        if ticker in self.sector_cache:
            return self.sector_cache[ticker]
        
        try:
            # Try Polygon API first
            sector = await self._get_sector_from_polygon(ticker)
            if not sector:
                # Fallback to yfinance
                sector = await self._get_sector_from_yfinance(ticker)
            
            if sector:
                self.sector_cache[ticker] = sector
                self._save_sector_cache()
            
            return sector
            
        except Exception as e:
            logger.error(f"Error fetching sector for {ticker}: {e}")
            return None
    
    async def _get_sector_from_polygon(self, ticker: str) -> Optional[str]:
        """Get sector from Polygon API"""
        try:
            url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={self.api_key}"
            
            async with self.semaphore:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=config.REQUEST_TIMEOUT)) as response:
                        if response.status == 200:
                            data = await response.json()
                            result = data.get('results', {})
                            
                            # Try different sector fields (same as main code)
                            if result.get('sector'):
                                return result['sector']
                            elif result.get('sic_description'):
                                return result['sic_description']
                            elif result.get('industry'):
                                return result['industry']
                            
                        elif response.status == 429:
                            # Rate limited, wait and retry
                            await asyncio.sleep(1)
                            return await self._get_sector_from_polygon(ticker)
                        else:
                            logger.debug(f"Polygon API returned {response.status} for {ticker}")
                            
        except Exception as e:
            logger.debug(f"Polygon API failed for {ticker}: {e}")
            
        return None
    
    async def _get_sector_from_yfinance(self, ticker: str) -> Optional[str]:
        """Get sector from yfinance fallback"""
        try:
            import yfinance as yf
            
            # Run in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            ticker_info = await loop.run_in_executor(
                None, 
                lambda: yf.Ticker(ticker).info
            )
            
            sector = ticker_info.get('sector')
            if sector:
                return sector
                
        except ImportError:
            logger.warning("yfinance not available for fallback sector data")
        except Exception as e:
            logger.debug(f"yfinance failed for {ticker}: {e}")
            
        return None
    
    async def get_ticker_sectors_bulk(self, tickers: List[str]) -> Dict[str, str]:
        """Get sector information for multiple tickers in bulk"""
        sector_data = {}
        failed_tickers = []
        
        # Create progress bar for sector data retrieval
        progress_bar = tqdm(total=len(tickers), desc="Fetching sector data", unit="ticker")
        
        # Process in batches to avoid rate limits
        batch_size = 100
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            batch_tasks = []
            
            for ticker in batch:
                task = asyncio.create_task(self.get_ticker_sector(ticker))
                batch_tasks.append((ticker, task))
            
            # Wait for all tasks in batch to complete
            for ticker, task in batch_tasks:
                try:
                    sector = await asyncio.wait_for(task, timeout=config.REQUEST_TIMEOUT)
                    if sector:
                        sector_data[ticker] = sector
                    else:
                        failed_tickers.append(ticker)
                except (asyncio.TimeoutError, Exception):
                    failed_tickers.append(ticker)
                
                progress_bar.update(1)
                progress_bar.set_postfix({'success': len(sector_data), 'failed': len(failed_tickers)})
            
            # Rate limiting between batches
            if i + batch_size < len(tickers):
                await asyncio.sleep(config.RATE_LIMIT_DELAY)
        
        progress_bar.close()
        
        logger.info(f"Sector data retrieval: {len(sector_data)} successful, {len(failed_tickers)} failed")
        
        return sector_data

# ======================== HISTORICAL TICKER MANAGER ======================== #
class HistoricalTickerManager:
    """Manages historical ticker CSV files and sector assignment"""
    
    def __init__(self):
        self.tickers_dir = config.HISTORICAL_TICKERS_DIR
        self.sectors_dir = config.BACKTEST_SECTORS_DIR
        
        # Create directories if they don't exist
        os.makedirs(self.sectors_dir, exist_ok=True)
        
    def get_all_historical_tickers(self) -> List[str]:
        """Get all ticker symbols from historical CSV files"""
        tickers = []
        
        if not os.path.exists(self.tickers_dir):
            logger.error(f"Historical tickers directory not found: {self.tickers_dir}")
            return tickers
        
        for filename in os.listdir(self.tickers_dir):
            if filename.endswith('.csv'):
                ticker = filename.replace('.csv', '')
                tickers.append(ticker)
        
        logger.info(f"Found {len(tickers)} historical ticker files")
        return tickers
    
    def get_ticker_data(self, ticker: str) -> pd.DataFrame:
        """Load historical data for a specific ticker"""
        filepath = os.path.join(self.tickers_dir, f"{ticker}.csv")
        try:
            df = pd.read_csv(filepath)
            return df
        except Exception as e:
            logger.error(f"Error loading data for {ticker}: {e}")
            return pd.DataFrame()
    
    def save_sector_data(self, sector_tickers: Dict[str, List[str]], sector_details: Dict[str, str]):
        """Save sector data to CSV files in backtest_sectors directory"""
        # Clean up existing sector files
        self._cleanup_sector_files()
        
        # Save each sector's tickers
        for sector, tickers in sector_tickers.items():
            if tickers:  # Only create files for sectors with tickers
                filename = f"{sector.lower().replace(' ', '_')}_sector.csv"
                filepath = os.path.join(self.sectors_dir, filename)
                
                # Create sector data with details
                sector_data = []
                for ticker in tickers:
                    detailed_sector = sector_details.get(ticker, 'Unknown')
                    sector_data.append({
                        'Ticker': ticker,
                        'Major_Sector': sector,
                        'Detailed_Sector': detailed_sector
                    })
                
                df = pd.DataFrame(sector_data)
                df.to_csv(filepath, index=False)
                logger.info(f"Saved {len(tickers)} tickers to {filename}")
        
        # Save summary file
        self._save_sector_summary(sector_tickers, sector_details)
    
    def _cleanup_sector_files(self):
        """Remove existing sector CSV files"""
        try:
            for filename in os.listdir(self.sectors_dir):
                if filename.endswith('_sector.csv') or filename == 'sector_summary.csv':
                    filepath = os.path.join(self.sectors_dir, filename)
                    os.remove(filepath)
        except Exception as e:
            logger.error(f"Error cleaning up sector files: {e}")
    
    def _save_sector_summary(self, sector_tickers: Dict[str, List[str]], sector_details: Dict[str, str]):
        """Save sector summary CSV file"""
        summary_data = []
        
        for sector, tickers in sector_tickers.items():
            summary_data.append({
                'Sector': sector,
                'Ticker_Count': len(tickers),
                'Tickers': ', '.join(tickers[:10]) + ('...' if len(tickers) > 10 else '')
            })
        
        df = pd.DataFrame(summary_data)
        summary_file = os.path.join(self.sectors_dir, 'sector_summary.csv')
        df.to_csv(summary_file, index=False)
        logger.info(f"Saved sector summary with {len(summary_data)} sectors")

# ======================== MAIN BACKTEST SECTOR SCANNER ======================== #
class BacktestSectorScanner:
    """Main scanner that fetches sector data for historical tickers"""
    
    def __init__(self):
        self.ticker_manager = HistoricalTickerManager()
        self.sector_fetcher = BacktestSectorDataFetcher()
        self.sector_mapping_manager = BacktestSectorMappingManager()
        
    async def scan_and_categorize_sectors(self):
        """Main method to scan all historical tickers and categorize by sector"""
        logger.info("Starting backtest sector scanning...")
        
        # Step 1: Get all historical tickers
        tickers = self.ticker_manager.get_all_historical_tickers()
        if not tickers:
            logger.error("No historical tickers found to process")
            return
        
        logger.info(f"Processing {len(tickers)} historical tickers")
        
        # Step 2: Fetch sector data for all tickers
        sector_details = await self.sector_fetcher.get_ticker_sectors_bulk(tickers)
        
        # Step 3: Map to major sectors
        sector_tickers = self._categorize_tickers_by_sector(sector_details)
        
        # Step 4: Save sector data to CSV files
        self.ticker_manager.save_sector_data(sector_tickers, sector_details)
        
        # Step 5: Display results
        self._display_results(sector_tickers, sector_details)
        
        logger.info("Backtest sector scanning completed successfully")
    
    def _categorize_tickers_by_sector(self, sector_details: Dict[str, str]) -> Dict[str, List[str]]:
        """Categorize tickers by their major sectors"""
        sector_tickers = {}
        
        for ticker, detailed_sector in sector_details.items():
            major_sector = self.sector_mapping_manager.get_major_sector(detailed_sector)
            
            if major_sector not in sector_tickers:
                sector_tickers[major_sector] = []
            
            sector_tickers[major_sector].append(ticker)
        
        # Sort tickers alphabetically within each sector
        for sector in sector_tickers:
            sector_tickers[sector].sort()
        
        return sector_tickers
    
    def _display_results(self, sector_tickers: Dict[str, List[str]], sector_details: Dict[str, str]):
        """Display scanning results"""
        print("\n" + "="*60)
        print("BACKTEST SECTOR SCANNING RESULTS")
        print("="*60)
        
        total_tickers = sum(len(tickers) for tickers in sector_tickers.values())
        print(f"Total Tickers Processed: {total_tickers}")
        print(f"Sectors Found: {len(sector_tickers)}")
        
        print(f"\nSector Breakdown:")
        for sector, tickers in sorted(sector_tickers.items(), key=lambda x: len(x[1]), reverse=True):
            print(f"  {sector}: {len(tickers)} tickers")
        
        # Show sample of tickers for each sector
        print(f"\nSample Tickers by Sector:")
        for sector, tickers in sorted(sector_tickers.items(), key=lambda x: len(x[1]), reverse=True):
            sample_tickers = tickers[:5]
            sample_str = ", ".join(sample_tickers)
            if len(tickers) > 5:
                sample_str += f" ... and {len(tickers) - 5} more"
            print(f"  {sector}: {sample_str}")
        
        # Show unknown sectors
        unknown_tickers = [ticker for ticker, sector in sector_details.items() if not sector or sector == 'Unknown']
        if unknown_tickers:
            print(f"\nTickers with Unknown Sectors: {len(unknown_tickers)}")
            print(f"  Sample: {', '.join(unknown_tickers[:10])}")
        
        print(f"\nOutput Directory: {config.BACKTEST_SECTORS_DIR}")
        print("="*60)

# ======================== MAIN EXECUTION ======================== #
async def main():
    """Main execution function"""
    scanner = BacktestSectorScanner()
    
    try:
        await scanner.scan_and_categorize_sectors()
    except Exception as e:
        logger.error(f"Backtest sector scanning failed: {e}")
        raise

def run_backtest_sector_scan():
    """Run the backtest sector scanner"""
    # Windows event loop policy for asyncio
    import sys
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    # Run the async main function
    asyncio.run(main())

if __name__ == "__main__":
    run_backtest_sector_scan()