# uses single ranking to limit positions per sector and manage risk

import alpaca_trade_api as tradeapi
import numpy as np
import pandas as pd
import asyncio
import aiohttp
import time
import os
import logging
import json
import threading
from datetime import datetime, timedelta
from urllib.parse import urlencode
from threading import Lock, Event, RLock
from collections import defaultdict
import sys
from tzlocal import get_localzone
import contextlib
from typing import List, Dict, Optional, Any, Tuple
import argparse
import asyncpg
from asyncpg.pool import Pool
import pandas_market_calendars as mcal
from functools import wraps
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import multiprocessing as mp
try:
    from polygon import RESTClient
except ImportError:
    try:
        from polygon.rest import RESTClient
    except ImportError:
        RESTClient = None
        import requests

import talib
from dataclasses import dataclass
from enum import Enum
from tqdm import tqdm
import certifi
import random

# ======================== GET MAIN DIRECTORY ======================== #
MAIN_DIR = os.path.dirname(os.path.abspath(__file__))

# ======================== EXCEPTIONS ======================== #
class TickerScannerError(Exception):
    """Base exception for Ticker Scanner"""
    pass

class DatabaseError(TickerScannerError):
    pass

class APIError(TickerScannerError):
    pass

class ConfigurationError(TickerScannerError):
    pass

# ======================== CONFIGURATION ======================== #
class Config:
    # API Configuration
    POLYGON_API_KEY = "ld1Poa63U6t4Y2MwOCA2JeKQyHVrmyg8"
    
    # Scanner Configuration
    COMPOSITE_INDICES = ["^IXAC"]
    MAX_CONCURRENT_REQUESTS = 200
    RATE_LIMIT_DELAY = 0.05
    SCAN_TIME = "08:30"
    
    # Error Handling Configuration
    MAX_RETRIES = 3
    RETRY_DELAY = 5
    
    # Database Configuration
    POSTGRES_HOST = "localhost"
    POSTGRES_PORT = 5432
    POSTGRES_DB = "stock_scanner"
    POSTGRES_USER = "hodumaru"
    POSTGRES_PASSWORD = "Leetkd214"
    
    # Market Calendar Configuration
    MARKET_CALENDAR = "NASDAQ"
    
    # Logging Configuration
    LOG_LEVEL = "INFO"
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Ranking Configuration
    RANKING_RESULTS_DIR = os.path.join(MAIN_DIR, "ranking_results")
    
    # Performance Configuration
    PARALLEL_WORKERS = min(mp.cpu_count(), 16)
    VECTORIZED_CALCULATIONS = True
    OPTIMIZED_BATCH_SIZE = 100
    MEMORY_EFFICIENT = True
    
    # Memory optimization
    AGGRESSIVE_MEMORY_OPTIMIZATION = True

    # Trading Configuration
    ALPACA_API_KEY = "PKA5JB3MHULE3GSCLP25PDLG7A"
    ALPACA_SECRET_KEY = "CJeBcRe1ZRrcegDxCvKxJ3CnPKxGvQatwhvsC8ZUKm5"
    ALPACA_BASE_URL = "https://paper-api.alpaca.markets"
    
    # Trade Schedule
    TRADE_EXECUTION_TIME = "09:30"

    def __init__(self):
        self.MAX_TICKERS_TO_RANK = None
    
    # File paths
    SECTOR_CACHE_FILE = os.path.join(MAIN_DIR, "sector_cache.json")
    SECTOR_BATCH_SIZE = 100
    SECTOR_REQUEST_DELAY = 1.0
    SECTOR_MAPPING_FILE = os.path.join(MAIN_DIR, "sector_mapping.json")
    RUN_STATUS_FILE = os.path.join(MAIN_DIR, "run_status.json")

    # Trading Configuration
    MAX_POSITIONS = 10  # Changed from sector-based to overall position limit
    MAX_PORTFOLIO_ALLOCATION = 0.25  # Max allocation per position
    MAX_POSITION_SIZE = 0.1
    STOP_LOSS_ATR_MULTIPLIER = 2.0
    TRAILING_STOP_PERCENT = 0.02

# Initialize config instance
config = Config()

# ======================== LOGGING ======================== #
def setup_logging():
    """Configure unified logging with file and console handlers - SINGLE FILE VERSION"""
    logs_dir = os.path.join(MAIN_DIR, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    
    logger = logging.getLogger("TickerScanner")
    
    # Check if logger already has handlers to prevent duplication
    if logger.handlers:
        return logger
    
    logger.setLevel(getattr(logging, config.LOG_LEVEL.upper()))
    
    formatter = logging.Formatter(config.LOG_FORMAT)
    
    # SINGLE main log file (without timestamp in filename)
    main_log_path = os.path.join(logs_dir, "ticker_scanner.log")
    main_handler = logging.FileHandler(main_log_path, encoding='utf-8')
    main_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Error handler (separate file for errors only)
    error_handler = logging.FileHandler(os.path.join(logs_dir, "errors.log"), encoding='utf-8')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(main_handler)
    logger.addHandler(console_handler)
    logger.addHandler(error_handler)
    
    # Prevent propagation to root logger to avoid duplicate logs
    logger.propagate = False
    
    return logger

# Initialize logger once
logger = setup_logging()

# ======================== TRADING ENUMS AND DATA CLASSES ======================== #
class TradeScore(Enum):
    STRONG = "STRONG"
    MODERATE = "MODERATE"
    WEAK = "WEAK"
    TEMPORARY_WEAK = "TEMPORARY_WEAK"

class VolatilityState(Enum):
    HIGH = "HIGH"
    LOW = "LOW"
    NORMAL = "NORMAL"

@dataclass
class PendingExecution:
    """Pending trade execution that displays before market open"""
    ticker: str
    signal_type: 'TradeSignalType'
    quantity: int
    estimated_price: float
    confidence: float
    ranking_score: float
    reason: str
    timestamp: str = None
    priority: int = 0
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

@dataclass
class SmartStopLoss:
    """Smart dynamic stop loss configuration"""
    ticker: str
    initial_stop_price: float
    current_stop_price: float
    stop_type: str  # "TRAILING", "VOLATILITY", "SUPPORT", "TIME_BASED"
    atr_multiplier: float = 2.0
    trailing_percent: float = 0.02
    last_updated: str = None
    activation_price: float = None  # Price where stop becomes active
    
    def __post_init__(self):
        if self.last_updated is None:
            self.last_updated = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# ======================== CAPITAL AND POSITION DETECTOR ======================== #
class CapitalPositionDetector:
    
    def __init__(self, trading_manager):
        self.trading_manager = trading_manager
        self.capital_info = None
        self.positions_info = None
        self.portfolio_summary = None
        self.detection_complete = False
    
    async def detect_capital_and_positions(self):
        """Scan for capital and positions - this runs FIRST before any other processing"""
        logger.info("🔍 SCANNING CAPITAL AND EXISTING POSITIONS FIRST...")
        
        try:
            # Step 1: Get capital information
            self.capital_info = await self.trading_manager.get_account_info()
            if not self.capital_info:
                logger.error("❌ Failed to retrieve capital information")
                return False
            
            # Step 2: Get existing positions
            self.positions_info = await self.trading_manager.get_positions()
            
            # Step 3: Get portfolio summary
            self.portfolio_summary = await self.trading_manager.get_portfolio_summary()
            
            self.detection_complete = True
            
            # Display the results immediately
            await self._display_detection_results()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error detecting capital and positions: {e}")
            return False
    
    async def _display_detection_results(self):
        """Display the capital and position detection results"""
        if not self.detection_complete:
            return
        
        print("\n" + "="*60)
        print("🏦 CAPITAL & POSITION DETECTION RESULTS")
        print("="*60)
        
        # Capital Information
        if self.capital_info:
            print(f"💰 PORTFOLIO VALUE: ${self.capital_info.get('portfolio_value', 0):.2f}")
            print(f"💵 AVAILABLE CASH: ${self.capital_info.get('cash', 0):.2f}")
            print(f"💳 BUYING POWER: ${self.capital_info.get('buying_power', 0):.2f}")
            print(f"📈 EQUITY: ${self.capital_info.get('equity', 0):.2f}")
            print(f"🔒 STATUS: {self.capital_info.get('status', 'N/A')}")
            print(f"🚫 TRADING BLOCKED: {self.capital_info.get('trading_blocked', 'N/A')}")
        else:
            print("❌ No capital information available")
        
        # Positions Information
        print(f"\n📊 EXISTING POSITIONS: {len(self.positions_info) if self.positions_info else 0}")
        if self.positions_info:
            print(f"{'Symbol':<10} {'Qty':<8} {'Avg Entry':<12} {'Current':<10} {'P/L':<10} {'P/L %':<8}")
            print("-" * 65)
            for pos in self.positions_info:
                pl_percent = pos.get('unrealized_plpc', 0) * 100
                pl_emoji = "🟢" if pos.get('unrealized_pl', 0) >= 0 else "🔴"
                print(f"{pos['symbol']:<10} {pos['quantity']:<8.2f} ${pos['avg_entry_price']:<11.2f} "
                      f"${pos['current_price']:<9.2f} {pl_emoji} ${pos['unrealized_pl']:<8.2f} {pl_percent:<7.2f}%")
        else:
            print("   No existing positions found")
        
        # Portfolio Summary
        if self.portfolio_summary:
            print(f"\n📋 PORTFOLIO ALLOCATION:")
            print(f"   💰 Cash: {self.portfolio_summary.get('cash_allocation_pct', 0):.1f}%")
            print(f"   📈 Positions: {self.portfolio_summary.get('positions_allocation_pct', 0):.1f}%")
            print(f"   📦 Total Positions: {self.portfolio_summary.get('number_of_positions', 0)}")
        
        print("="*60)
        print("✅ CAPITAL AND POSITION DETECTION COMPLETE")
        print("="*60)
    
    def get_available_capital(self) -> float:
        """Get available capital for trading"""
        if self.capital_info:
            return self.capital_info.get('buying_power', 0.0)
        return 0.0
    
    def get_existing_positions(self) -> List[Dict]:
        """Get list of existing positions"""
        return self.positions_info or []
    
    def has_position(self, ticker: str) -> bool:
        """Check if we have an existing position for a ticker"""
        if not self.positions_info:
            return False
        return any(pos['symbol'] == ticker for pos in self.positions_info)
    
    def get_position_quantity(self, ticker: str) -> float:
        """Get quantity of existing position for a ticker"""
        if not self.positions_info:
            return 0.0
        for position in self.positions_info:
            if position['symbol'] == ticker:
                return position['quantity']
        return 0.0

# ======================== ALPACA TRADING MANAGER ======================== #
class AlpacaTradingManager:
    """Manages Alpaca trading operations including capital and position detection"""
    
    def __init__(self):
        self.api_key = config.ALPACA_API_KEY
        self.secret_key = config.ALPACA_SECRET_KEY
        self.base_url = config.ALPACA_BASE_URL
        self.api = None
        self.initialized = False
        self.account_info = None
        self.positions = []
        
    async def initialize(self):
        """Initialize Alpaca API connection"""
        try:
            if not self.api_key or not self.secret_key:
                logger.error("Alpaca API credentials not configured")
                return False
                
            # Fix SSL certificate issue
            os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
            os.environ['SSL_CERT_FILE'] = certifi.where()
            
            self.api = tradeapi.REST(self.api_key, self.secret_key, self.base_url, api_version='v2')
            
            # Test connection
            account = self.api.get_account()
            self.initialized = True
            logger.info("✅ Alpaca trading manager initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize Alpaca trading manager: {e}")
            self.initialized = False
            return False
    
    def safe_get_account_attr(self, account, attr_name, default="N/A", is_currency=False):
        """Safely get account attribute with error handling"""
        try:
            value = getattr(account, attr_name)
            if value is None:
                return default
            if is_currency:
                return float(value)
            return value
        except AttributeError:
            return default
        except (ValueError, TypeError):
            return default
    
    async def get_account_info(self):
        """Get account information including capital"""
        if not self.initialized:
            await self.initialize()
            if not self.initialized:
                return None
        
        try:
            account = self.api.get_account()
            
            self.account_info = {
                'account_number': self.safe_get_account_attr(account, 'account_number'),
                'portfolio_value': self.safe_get_account_attr(account, 'portfolio_value', is_currency=True),
                'buying_power': self.safe_get_account_attr(account, 'buying_power', is_currency=True),
                'cash': self.safe_get_account_attr(account, 'cash', is_currency=True),
                'equity': self.safe_get_account_attr(account, 'equity', is_currency=True),
                'initial_margin': self.safe_get_account_attr(account, 'initial_margin', is_currency=True),
                'maintenance_margin': self.safe_get_account_attr(account, 'maintenance_margin', is_currency=True),
                'day_trade_count': self.safe_get_account_attr(account, 'day_trade_count'),
                'status': self.safe_get_account_attr(account, 'status'),
                'trading_blocked': self.safe_get_account_attr(account, 'trading_blocked'),
                'transfers_blocked': self.safe_get_account_attr(account, 'transfers_blocked')
            }
            
            logger.info(f"Account info: ${self.account_info['cash']:.2f} cash, ${self.account_info['buying_power']:.2f} buying power")
            return self.account_info
            
        except Exception as e:
            logger.error(f"Error fetching account info: {e}")
            return None
    
    async def get_positions(self):
        """Get all current positions"""
        if not self.initialized:
            await self.initialize()
            if not self.initialized:
                return []
        
        try:
            positions = self.api.list_positions()
            self.positions = []
            
            for position in positions:
                try:
                    position_data = {
                        'symbol': position.symbol,
                        'quantity': float(position.qty),
                        'avg_entry_price': float(position.avg_entry_price),
                        'current_price': float(position.current_price),
                        'market_value': float(position.market_value),
                        'unrealized_pl': float(position.unrealized_pl),
                        'unrealized_plpc': float(position.unrealized_plpc),
                        'side': 'LONG' if float(position.qty) > 0 else 'SHORT'
                    }
                    self.positions.append(position_data)
                except (AttributeError, ValueError, TypeError) as e:
                    logger.warning(f"Error processing position {getattr(position, 'symbol', 'Unknown')}: {e}")
                    continue
            
            logger.info(f"Found {len(self.positions)} existing positions")
            return self.positions
            
        except Exception as e:
            logger.error(f"Error fetching positions: {e}")
            return []
    
    async def has_existing_position(self, ticker: str) -> bool:
        """Check if we have an existing position for a ticker"""
        if not self.positions:
            await self.get_positions()
        
        return any(pos['symbol'] == ticker for pos in self.positions)
    
    async def get_position_quantity(self, ticker: str) -> float:
        """Get quantity of existing position for a ticker"""
        if not self.positions:
            await self.get_positions()
        
        for position in self.positions:
            if position['symbol'] == ticker:
                return position['quantity']
        return 0.0
    
    async def get_available_capital(self) -> float:
        """Get available buying power for trading"""
        account_info = await self.get_account_info()
        if account_info:
            return account_info.get('buying_power', 0.0)
        return 0.0
    
    async def get_portfolio_summary(self):
        """Get portfolio summary"""
        account_info = await self.get_account_info()
        positions = await self.get_positions()
        
        if not account_info:
            return None
        
        summary = {
            'total_portfolio_value': account_info.get('portfolio_value', 0),
            'total_positions_value': sum(pos.get('market_value', 0) for pos in positions),
            'available_cash': account_info.get('cash', 0),
            'buying_power': account_info.get('buying_power', 0),
            'number_of_positions': len(positions),
            'account_status': account_info.get('status'),
            'trading_blocked': account_info.get('trading_blocked', False)
        }
        
        # Calculate allocations
        if summary['total_portfolio_value'] > 0:
            summary['cash_allocation_pct'] = (summary['available_cash'] / summary['total_portfolio_value']) * 100
            summary['positions_allocation_pct'] = (summary['total_positions_value'] / summary['total_portfolio_value']) * 100
        
        return summary
    
    async def display_account_status(self):
        """Display account status in a formatted way"""
        account_info = await self.get_account_info()
        positions = await self.get_positions()
        portfolio_summary = await self.get_portfolio_summary()
        
        if not account_info:
            logger.error("Cannot display account status: No account info available")
            return
        
        print("\n" + "="*50)
        print("ALPACA ACCOUNT STATUS")
        print("="*50)
        
        print(f"Account Number: {account_info.get('account_number', 'N/A')}")
        print(f"Portfolio Value: ${account_info.get('portfolio_value', 0):.2f}")
        print(f"Buying Power: ${account_info.get('buying_power', 0):.2f}")
        print(f"Cash: ${account_info.get('cash', 0):.2f}")
        print(f"Equity: ${account_info.get('equity', 0):.2f}")
        print(f"Status: {account_info.get('status', 'N/A')}")
        print(f"Trading Blocked: {account_info.get('trading_blocked', 'N/A')}")
        
        print(f"\nPOSITIONS: {len(positions)}")
        if positions:
            print(f"{'Symbol':<10} {'Qty':<8} {'Avg Entry':<12} {'Current':<10} {'P/L':<10} {'P/L %':<8}")
            print("-" * 60)
            for pos in positions:
                pl_percent = pos.get('unrealized_plpc', 0) * 100
                print(f"{pos['symbol']:<10} {pos['quantity']:<8.2f} ${pos['avg_entry_price']:<11.2f} "
                      f"${pos['current_price']:<9.2f} ${pos['unrealized_pl']:<9.2f} {pl_percent:<7.2f}%")
        else:
            print("No positions found")
        
        if portfolio_summary:
            print(f"\nPORTFOLIO ALLOCATION:")
            print(f"Cash: {portfolio_summary.get('cash_allocation_pct', 0):.1f}%")
            print(f"Positions: {portfolio_summary.get('positions_allocation_pct', 0):.1f}%")
        
        print("="*50)

# ======================== TRADING EXECUTION SYSTEM ======================== #
class RiskManager:
    """Manages overall risk and position allocation without sectors"""
    
    def __init__(self, max_positions: int = 10, max_allocation: float = 0.25):
        self.max_positions = max_positions
        self.max_allocation = max_allocation
        self.current_positions = []
        
    async def calculate_current_allocation(self, positions: List[Dict]) -> float:
        """Calculate current portfolio allocation"""
        total_portfolio_value = 0
        total_positions_value = 0
        
        for position in positions:
            market_value = position.get('market_value', 0)
            total_positions_value += market_value
            total_portfolio_value += market_value
        
        # Add cash to portfolio value
        account_info = await self._get_account_info()
        if account_info:
            total_portfolio_value += account_info.get('cash', 0)
        
        if total_portfolio_value > 0:
            return total_positions_value / total_portfolio_value
        return 0.0
    
    async def can_add_position(self, position_size: float, 
                             total_portfolio_value: float,
                             current_positions_count: int) -> Tuple[bool, str]:
        """Check if we can add a position considering overall limits"""
        if total_portfolio_value <= 0:
            return False, "No portfolio value"
            
        proposed_allocation = position_size / total_portfolio_value
        
        # Check overall allocation limit
        current_allocation = await self.calculate_current_allocation(self.current_positions)
        if current_allocation + proposed_allocation > self.max_allocation:
            return False, f"Portfolio allocation would exceed {self.max_allocation*100}%"
        
        # Check maximum positions
        if current_positions_count >= self.max_positions:
            return False, f"Already have {self.max_positions} positions"
        
        return True, f"OK - Current allocation: {current_allocation*100:.1f}%"
    
    async def _get_account_info(self):
        """Get account information - placeholder implementation"""
        # This would be implemented with your actual account info retrieval
        return {'cash': 0}

class DynamicStopLossManager:
    """Manages smart, dynamic stop losses using real-time data"""
    
    def __init__(self, data_provider: 'DataProvider', volatility_lookback: int = 20):
        self.data_provider = data_provider
        self.volatility_lookback = volatility_lookback
        self.active_stops: Dict[str, SmartStopLoss] = {}
        self.volatility_state = VolatilityState.NORMAL
        
    async def calculate_initial_stop_loss(self, ticker: str, entry_price: float, 
                                        signal_type: 'TradeSignalType') -> SmartStopLoss:
        """Calculate initial smart stop loss"""
        # Get recent data for volatility calculation
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        
        df = await self.data_provider.get_historical_bars_optimized(
            ticker, "1day", start_date, end_date
        )
        
        if df.empty or len(df) < 10:
            # Fallback to percentage-based stop
            stop_percent = 0.08 if signal_type == TradeSignalType.BUY else 0.06
            stop_price = entry_price * (1 - stop_percent) if signal_type == TradeSignalType.BUY else entry_price * (1 + stop_percent)
            return SmartStopLoss(ticker, stop_price, stop_price, "PERCENTAGE_BASED")
        
        # Calculate ATR for volatility-based stop
        atr = await self._calculate_atr(df)
        current_volatility = await self._calculate_volatility(df)
        
        # Determine stop type and multiplier based on volatility
        if current_volatility > 0.04:  # High volatility
            multiplier = 2.5
            stop_type = "VOLATILITY_HIGH"
        elif current_volatility < 0.015:  # Low volatility
            multiplier = 1.5
            stop_type = "VOLATILITY_LOW"
        else:  # Normal volatility
            multiplier = 2.0
            stop_type = "VOLATILITY_NORMAL"
        
        # Calculate stop price
        if signal_type == TradeSignalType.BUY:
            stop_price = entry_price - (atr * multiplier)
            activation_price = entry_price * 0.98  # Activate when price drops 2%
        else:  # SHORT
            stop_price = entry_price + (atr * multiplier)
            activation_price = entry_price * 1.02  # Activate when price rises 2%
        
        return SmartStopLoss(
            ticker=ticker,
            initial_stop_price=stop_price,
            current_stop_price=stop_price,
            stop_type=stop_type,
            atr_multiplier=multiplier,
            activation_price=activation_price
        )
    
    async def update_stop_loss(self, ticker: str, current_price: float, 
                             signal_type: 'TradeSignalType') -> Optional[float]:
        """Update trailing stop loss based on current price"""
        if ticker not in self.active_stops:
            return None
        
        stop_loss = self.active_stops[ticker]
        
        # Only update if stop is activated
        if stop_loss.activation_price and (
            (signal_type == TradeSignalType.BUY and current_price < stop_loss.activation_price) or
            (signal_type == TradeSignalType.SELL and current_price > stop_loss.activation_price)
        ):
            return stop_loss.current_stop_price
        
        # Update trailing stop for long positions
        if signal_type == TradeSignalType.BUY and stop_loss.stop_type == "TRAILING":
            new_stop = current_price * (1 - stop_loss.trailing_percent)
            if new_stop > stop_loss.current_stop_price:
                stop_loss.current_stop_price = new_stop
                stop_loss.last_updated = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Update trailing stop for short positions
        elif signal_type == TradeSignalType.SELL and stop_loss.stop_type == "TRAILING":
            new_stop = current_price * (1 + stop_loss.trailing_percent)
            if new_stop < stop_loss.current_stop_price:
                stop_loss.current_stop_price = new_stop
                stop_loss.last_updated = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return stop_loss.current_stop_price
    
    async def check_stop_loss(self, ticker: str, current_price: float, 
                            signal_type: 'TradeSignalType') -> bool:
        """Check if stop loss has been triggered"""
        if ticker not in self.active_stops:
            return False
        
        stop_loss = self.active_stops[ticker]
        
        if signal_type == TradeSignalType.BUY:
            return current_price <= stop_loss.current_stop_price
        else:  # SHORT
            return current_price >= stop_loss.current_stop_price
    
    async def _calculate_atr(self, df: pd.DataFrame, period: int = 14) -> float:
        """Calculate Average True Range"""
        try:
            high = df['high'].values
            low = df['low'].values
            close = df['close'].values
            
            tr = np.zeros(len(high))
            for i in range(1, len(high)):
                tr1 = high[i] - low[i]
                tr2 = abs(high[i] - close[i-1])
                tr3 = abs(low[i] - close[i-1])
                tr[i] = max(tr1, tr2, tr3)
            
            atr = np.mean(tr[-period:])
            return atr
        except Exception:
            return 0.02  # Default 2% ATR
    
    async def _calculate_volatility(self, df: pd.DataFrame) -> float:
        """Calculate price volatility"""
        try:
            returns = df['close'].pct_change().dropna()
            volatility = returns.std() * np.sqrt(252)  # Annualized
            return volatility
        except Exception:
            return 0.02  # Default 2% volatility

class TradeExitManager:
    """Manages trade exit logic with weak signal detection"""
    
    def __init__(self, confidence_threshold: float = 0.4, temporary_weak_threshold: float = 0.3):
        self.confidence_threshold = confidence_threshold
        self.temporary_weak_threshold = temporary_weak_threshold
        self.temporary_weak_signals: Dict[str, List[float]] = defaultdict(list)
        self.max_weak_periods = 3  # Number of periods to consider for temporary weakness
    
    async def evaluate_trade_score(self, ticker: str, current_ranking: 'TickerRanking', 
                                 position_data: Dict) -> TradeScore:
        """Evaluate if a trade should be exited based on current score"""
        confidence = current_ranking.confidence
        total_score = current_ranking.total_score
        
        # Strong signal - keep position
        if confidence > 0.7 and total_score > 0.6:
            return TradeScore.STRONG
        
        # Moderate signal - monitor closely
        if confidence > 0.5 and total_score > 0.4:
            return TradeScore.MODERATE
        
        # Check for temporary weakness
        if await self._is_temporary_weakness(ticker, confidence, total_score):
            return TradeScore.TEMPORARY_WEAK
        
        # Genuinely weak signal - exit
        return TradeScore.WEAK
    
    async def _is_temporary_weakness(self, ticker: str, confidence: float, 
                                   total_score: float) -> bool:
        """Determine if current weakness is temporary"""
        # Store current values
        self.temporary_weak_signals[ticker].append((confidence, total_score))
        
        # Keep only recent history
        if len(self.temporary_weak_signals[ticker]) > self.max_weak_periods:
            self.temporary_weak_signals[ticker].pop(0)
        
        history = self.temporary_weak_signals[ticker]
        
        # Need sufficient history to determine
        if len(history) < 2:
            return True  # Assume temporary until we have more data
        
        # Check if this is a sudden drop from previously strong signals
        strong_periods = sum(1 for conf, score in history[:-1] 
                           if conf > 0.6 and score > 0.5)
        
        current_weak = confidence < self.temporary_weak_threshold
        
        # If we had strong periods recently and current is weak, likely temporary
        if strong_periods >= 1 and current_weak:
            return True
        
        # Check trend - if confidence is improving even from low levels
        if len(history) >= 3:
            recent_confidences = [conf for conf, score in history[-3:]]
            if (recent_confidences[-1] > recent_confidences[-2] > recent_confidences[-3] and
                recent_confidences[-1] > self.temporary_weak_threshold):
                return True
        
        return False
    
    async def should_exit_trade(self, ticker: str, trade_score: TradeScore, 
                              position_pnl: float) -> Tuple[bool, str]:
        """Determine if trade should be exited"""
        if trade_score == TradeScore.STRONG:
            return False, "Strong signal - maintain position"
        
        elif trade_score == TradeScore.MODERATE:
            # For moderate signals, consider P&L
            if position_pnl < -0.05:  # 5% loss
                return True, f"Moderate signal with {position_pnl:.1%} loss - exit"
            return False, "Moderate signal - monitor"
        
        elif trade_score == TradeScore.TEMPORARY_WEAK:
            # For temporary weakness, be more patient with losses
            if position_pnl < -0.08:  # 8% loss
                return True, f"Temporary weak signal with {position_pnl:.1%} loss - exit"
            return False, "Temporary weakness - hold position"
        
        elif trade_score == TradeScore.WEAK:
            # Exit weak signals unless they're profitable
            if position_pnl <= 0.02:  # Exit if not making good profit
                return True, f"Weak signal with {position_pnl:.1%} P&L - exit"
            return False, "Weak signal but profitable - monitor closely"
        
        return False, "Unknown signal state"

class RealTimeDataManager:
    """Manages real-time data streaming for stop loss and monitoring"""
    
    def __init__(self, alpaca_manager: AlpacaTradingManager, data_provider: 'DataProvider'):
        self.alpaca_manager = alpaca_manager
        self.data_provider = data_provider
        self.connected = False
        self.last_prices: Dict[str, float] = {}
        self.price_lock = asyncio.Lock()  # Change to asyncio.Lock
        self.volatility_monitor = VolatilityMonitor()
    
    async def connect(self):
        """Connect to real-time data stream"""
        try:
            # Initialize Alpaca streaming if available
            if hasattr(self.alpaca_manager, 'api'):
                # Alpaca WebSocket connection would go here
                logger.info("Real-time data manager initialized (Alpaca)")
            else:
                logger.info("Real-time data manager initialized (Polygon)")
            
            self.connected = True
        except Exception as e:
            logger.error(f"Failed to initialize real-time data: {e}")
            self.connected = False
    
    async def get_current_price(self, ticker: str) -> Optional[float]:
        """Get current price for a ticker"""
        try:
            # Try to get from last prices first
            async with self.price_lock:  # This now works with asyncio.Lock
                if ticker in self.last_prices:
                    return self.last_prices[ticker]
            
            # Fallback to API call
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
            
            df = await self.data_provider.get_historical_bars_optimized(
                ticker, "1day", start_date, end_date
            )
            
            if not df.empty:
                current_price = df['close'].iloc[-1]
                async with self.price_lock:
                    self.last_prices[ticker] = current_price
                return current_price
            
            return None
        except Exception as e:
            logger.error(f"Error getting current price for {ticker}: {e}")
            return None
    
    async def update_price(self, ticker: str, price: float):
        """Update price for a ticker (called by streaming)"""
        async with self.price_lock:  # This now works with asyncio.Lock
            self.last_prices[ticker] = price
    
    async def monitor_volatility(self) -> VolatilityState:
        """Monitor market volatility state"""
        return await self.volatility_monitor.get_volatility_state()

class VolatilityMonitor:
    """Monitors market volatility to adjust scanning intervals"""
    
    def __init__(self, lookback_days: int = 20, high_vol_threshold: float = 0.25):
        self.lookback_days = lookback_days
        self.high_vol_threshold = high_vol_threshold  # VIX threshold for high volatility
        self.current_state = VolatilityState.NORMAL
    
    async def get_volatility_state(self) -> VolatilityState:
        """Get current market volatility state"""
        try:
            # Use VIX as volatility indicator
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=self.lookback_days)).strftime("%Y-%m-%d")
            
            # This would fetch VIX data - for now using placeholder
            vix_data = await self._get_vix_data(start_date, end_date)
            
            if vix_data and len(vix_data) > 0:
                current_vix = vix_data[-1]  # Most recent VIX value
                
                if current_vix > self.high_vol_threshold:
                    self.current_state = VolatilityState.HIGH
                elif current_vix < 0.15:  # Low volatility threshold
                    self.current_state = VolatilityState.LOW
                else:
                    self.current_state = VolatilityState.NORMAL
            
            return self.current_state
            
        except Exception as e:
            logger.error(f"Error monitoring volatility: {e}")
            return VolatilityState.NORMAL
    
    async def _get_vix_data(self, start_date: str, end_date: str) -> List[float]:
        """Get VIX data - placeholder implementation"""
        # In practice, implement this to fetch actual VIX data
        # Returning mock data for demonstration
        return [0.18, 0.19, 0.20, 0.22, 0.25]  # Mock VIX values
    
    def get_scan_interval(self) -> int:
        """Get appropriate scan interval based on volatility"""
        if self.current_state == VolatilityState.HIGH:
            return 10  # 10 minutes during high volatility
        elif self.current_state == VolatilityState.LOW:
            return 30  # 30 minutes during low volatility
        else:
            return 20  # 20 minutes during normal volatility

class PendingExecutionManager:
    """Manages pending trade executions that display before market open"""
    
    def __init__(self):
        self.pending_executions: List[PendingExecution] = []
        self.execution_lock = asyncio.Lock()  
    
    async def add_pending_execution(self, execution: PendingExecution):
        """Add a pending execution"""
        async with self.execution_lock:  # This now works with asyncio.Lock
            self.pending_executions.append(execution)
    
    async def get_pending_executions(self) -> List[PendingExecution]:
        """Get all pending executions"""
        async with self.execution_lock:  # This now works with asyncio.Lock
            return sorted(self.pending_executions, key=lambda x: x.priority, reverse=True)
    
    async def clear_executed(self, ticker: str):
        """Remove pending execution after it's been executed"""
        async with self.execution_lock:  # This now works with asyncio.Lock
            self.pending_executions = [e for e in self.pending_executions if e.ticker != ticker]
    
    async def display_pending_executions(self):
        """Display all pending executions in a formatted way"""
        pending = await self.get_pending_executions()
        
        if not pending:
            print("No pending executions")
            return
        
        print("\n" + "="*80)
        print("🔄 PENDING TRADE EXECUTIONS (Pre-Market Analysis)")
        print("="*80)
        
        for i, execution in enumerate(pending, 1):
            signal_emoji = "🟢" if execution.signal_type == TradeSignalType.BUY else "🔴"
            priority_stars = "★" * execution.priority
            
            print(f"{i:2d}. {signal_emoji} {execution.ticker:<6} | "
                  f"Qty: {execution.quantity:>4} | "
                  f"Est. Price: ${execution.estimated_price:>7.2f} | "
                  f"Conf: {execution.confidence:.2f} | "
                  f"Score: {execution.ranking_score:.3f} | "
                  f"Priority: {priority_stars}")
            print(f"    Reason: {execution.reason}")
            print(f"    Analysis Time: {execution.timestamp}")
            print()
        
        print(f"Total Pending Executions: {len(pending)}")
        print("="*80)

class DynamicPositionSizer:
    """Calculates dynamic position sizes based on capital and risk"""
    
    def __init__(self, max_position_size: float = 0.1, max_portfolio_risk: float = 0.02):
        self.max_position_size = max_position_size  # Max 10% per position
        self.max_portfolio_risk = max_portfolio_risk  # Max 2% portfolio risk per trade
    
    async def calculate_position_size(self, ticker: str, entry_price: float, 
                                    stop_price: float, available_capital: float,
                                    confidence: float, signal_type: 'TradeSignalType') -> int:
        """Calculate position size based on risk and capital"""
        if available_capital <= 0:
            return 0
        
        # Calculate risk per share
        if signal_type == TradeSignalType.BUY:
            risk_per_share = entry_price - stop_price
        else:  # SHORT
            risk_per_share = stop_price - entry_price
        
        if risk_per_share <= 0:
            return 0
        
        # Adjust max risk based on confidence
        confidence_multiplier = min(1.0, confidence * 1.5)  # Up to 1.5x for high confidence
        adjusted_max_risk = self.max_portfolio_risk * confidence_multiplier
        
        # Calculate position size based on risk
        risk_based_size = (available_capital * adjusted_max_risk) / risk_per_share
        
        # Calculate position size based on maximum allocation
        allocation_based_size = (available_capital * self.max_position_size) / entry_price
        
        # Use the smaller of the two
        position_size = min(risk_based_size, allocation_based_size)
        
        # Round down to whole shares
        position_size = int(position_size)
        
        # Ensure minimum of 1 share if we're taking the position
        if position_size == 0 and confidence > 0.6:
            position_size = 1
        
        return position_size

class TradeExecutor:
    """Main trade execution system that coordinates all components"""
    
    def __init__(self, scanner: 'PolygonTickerScanner'):
        self.scanner = scanner
        self.alpaca_manager = scanner.alpaca_manager
        self.data_provider = DataProvider(config.POLYGON_API_KEY)
        
        # Initialize all managers
        self.risk_manager = RiskManager()
        self.stop_loss_manager = DynamicStopLossManager(self.data_provider)
        self.exit_manager = TradeExitManager()
        self.real_time_manager = RealTimeDataManager(self.alpaca_manager, self.data_provider)
        self.pending_execution_manager = PendingExecutionManager()
        self.position_sizer = DynamicPositionSizer()
        self.volatility_monitor = VolatilityMonitor()
        
        # Execution state
        self.is_running = False
        self.last_scan_time = None
        self.active_monitoring: Dict[str, asyncio.Task] = {}
        
        logger.info("Trade Executor initialized")
    
    async def start(self):
        """Start the trade execution system"""
        await self.real_time_manager.connect()
        self.is_running = True
        
        # Start monitoring existing positions
        await self._start_position_monitoring()
        
        logger.info("Trade Executor started")
    
    async def stop(self):
        """Stop the trade execution system"""
        self.is_running = False
        
        # Cancel all monitoring tasks
        for task in self.active_monitoring.values():
            task.cancel()
        
        self.active_monitoring.clear()
        logger.info("Trade Executor stopped")
    
    async def execute_trades_based_on_rankings(self, rankings: List['TickerRanking'], 
                                             max_trades: int = 5):
        """Execute trades based on ranking analysis"""
        if not self.is_running:
            logger.warning("Trade executor not running")
            return
        
        # Get current portfolio state
        available_capital = await self.alpaca_manager.get_available_capital()
        existing_positions = await self.alpaca_manager.get_positions()
        portfolio_summary = await self.alpaca_manager.get_portfolio_summary()
        
        if not portfolio_summary or available_capital <= 0:
            logger.warning("No available capital for trading")
            return
        
        total_portfolio_value = portfolio_summary.get('total_portfolio_value', 0)
        
        # Update risk manager with current positions
        self.risk_manager.current_positions = existing_positions
        
        executed_trades = 0
        pending_executions = []
        
        for ranking in rankings:
            if executed_trades >= max_trades:
                break
            
            if not await self._should_execute_trade(ranking, existing_positions):
                continue
            
            # Calculate position size
            current_price = ranking.current_price
            stop_loss = await self.stop_loss_manager.calculate_initial_stop_loss(
                ranking.ticker, current_price, 
                TradeSignalType.BUY if ranking.signal_type == "LONG" else TradeSignalType.SELL
            )
            
            position_size = await self.position_sizer.calculate_position_size(
                ranking.ticker, current_price, stop_loss.current_stop_price,
                available_capital, ranking.confidence,
                TradeSignalType.BUY if ranking.signal_type == "LONG" else TradeSignalType.SELL
            )
            
            if position_size <= 0:
                continue
            
            # Check overall limits
            position_value = position_size * current_price
            can_trade, reason = await self.risk_manager.can_add_position(
                position_value, total_portfolio_value, len(existing_positions)
            )
            
            if not can_trade:
                logger.info(f"Risk limit prevented trade for {ranking.ticker}: {reason}")
                continue
            
            # Create pending execution
            execution = PendingExecution(
                ticker=ranking.ticker,
                signal_type=TradeSignalType.BUY if ranking.signal_type == "LONG" else TradeSignalType.SELL,
                quantity=position_size,
                estimated_price=current_price,
                confidence=ranking.confidence,
                ranking_score=ranking.total_score,
                reason=f"Ranked #{ranking.rank} with score {ranking.total_score:.3f}",
                priority=int(ranking.total_score * 10)
            )
            
            pending_executions.append(execution)
            executed_trades += 1
        
        # Add all pending executions
        for execution in pending_executions:
            await self.pending_execution_manager.add_pending_execution(execution)
        
        # Display pending executions
        await self.pending_execution_manager.display_pending_executions()
        
        logger.info(f"Created {len(pending_executions)} pending executions")
    
    async def _should_execute_trade(self, ranking: 'TickerRanking', 
                                  existing_positions: List[Dict]) -> bool:
        """Determine if a trade should be executed"""
        # Check if we already have a position
        existing_position = any(pos['symbol'] == ranking.ticker for pos in existing_positions)
        if existing_position:
            return False
        
        # Check signal strength
        if ranking.total_score < 0.4 or ranking.confidence < 0.5:
            return False
        
        # For SHORT signals, require higher confidence
        if ranking.signal_type == "SHORT" and ranking.confidence < 0.6:
            return False
        
        return True
    
    async def _start_position_monitoring(self):
        """Start monitoring all existing positions"""
        positions = await self.alpaca_manager.get_positions()
        
        for position in positions:
            ticker = position['symbol']
            if ticker not in self.active_monitoring:
                self.active_monitoring[ticker] = asyncio.create_task(
                    self._monitor_single_position(ticker)
                )
    
    async def _monitor_single_position(self, ticker: str):
        """Monitor a single position for stop loss and exit signals"""
        while self.is_running:
            try:
                # Get current position data
                positions = await self.alpaca_manager.get_positions()
                position = next((p for p in positions if p['symbol'] == ticker), None)
                
                if not position:
                    logger.info(f"Position for {ticker} no longer exists, stopping monitoring")
                    break
                
                # Get current price
                current_price = await self.real_time_manager.get_current_price(ticker)
                if not current_price:
                    await asyncio.sleep(60)  # Wait 1 minute if no price
                    continue
                
                # Determine signal type
                quantity = position['quantity']
                signal_type = TradeSignalType.BUY if float(quantity) > 0 else TradeSignalType.SELL
                
                # Check stop loss
                if await self.stop_loss_manager.check_stop_loss(ticker, current_price, signal_type):
                    logger.info(f"Stop loss triggered for {ticker} at ${current_price:.2f}")
                    await self._exit_position(ticker, "Stop loss triggered")
                    break
                
                # Update trailing stop
                await self.stop_loss_manager.update_stop_loss(ticker, current_price, signal_type)
                
                # Check exit signals (less frequently)
                if await self._should_check_exit_signal():
                    await self._check_exit_signals(ticker, position, current_price)
                
                # Wait based on volatility
                volatility_state = await self.real_time_manager.monitor_volatility()
                scan_interval = self.volatility_monitor.get_scan_interval()
                
                await asyncio.sleep(scan_interval * 60)  # Convert to seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error monitoring position {ticker}: {e}")
                await asyncio.sleep(60)
    
    async def _should_check_exit_signal(self) -> bool:
        """Determine if we should check exit signals (less frequent than stop loss)"""
        # Check every 5 monitoring cycles or based on some condition
        return random.random() < 0.2  # 20% chance each cycle
    
    async def _check_exit_signals(self, ticker: str, position: Dict, current_price: float):
        """Check if position should be exited based on trade score"""
        try:
            # Get current ranking for the ticker
            rankings = await self.scanner.combined_ranking_engine.rank_all_tickers_optimized(100)
            current_ranking = next((r for r in rankings if r.ticker == ticker), None)
            
            if not current_ranking:
                return
            
            # Calculate P&L
            entry_price = float(position['avg_entry_price'])
            if float(position['quantity']) > 0:  # LONG
                pnl_pct = (current_price - entry_price) / entry_price
            else:  # SHORT
                pnl_pct = (entry_price - current_price) / entry_price
            
            # Evaluate trade score
            trade_score = await self.exit_manager.evaluate_trade_score(
                ticker, current_ranking, position
            )
            
            # Check if should exit
            should_exit, reason = await self.exit_manager.should_exit_trade(
                ticker, trade_score, pnl_pct
            )
            
            if should_exit:
                logger.info(f"Exit signal for {ticker}: {reason}")
                await self._exit_position(ticker, reason)
        
        except Exception as e:
            logger.error(f"Error checking exit signals for {ticker}: {e}")
    
    async def _exit_position(self, ticker: str, reason: str):
        """Exit a position"""
        try:
            positions = await self.alpaca_manager.get_positions()
            position = next((p for p in positions if p['symbol'] == ticker), None)
            
            if not position:
                return
            
            quantity = abs(float(position['quantity']))
            
            # Determine order side based on position
            if float(position['quantity']) > 0:  # LONG position, need to sell
                order_side = 'sell'
            else:  # SHORT position, need to buy to cover
                order_side = 'buy'
            
            # Place market order to exit
            api = self.alpaca_manager.api
            if api:
                api.submit_order(
                    symbol=ticker,
                    qty=quantity,
                    side=order_side,
                    type='market',
                    time_in_force='day'
                )
                
                logger.info(f"Exited position for {ticker}: {quantity} shares ({reason})")
                
                # Remove from monitoring
                if ticker in self.active_monitoring:
                    self.active_monitoring[ticker].cancel()
                    del self.active_monitoring[ticker]
                
                # Remove stop loss
                if ticker in self.stop_loss_manager.active_stops:
                    del self.stop_loss_manager.active_stops[ticker]
            
        except Exception as e:
            logger.error(f"Error exiting position for {ticker}: {e}")
    
    async def execute_pending_trades(self):
        """Execute all pending trades"""
        pending_executions = await self.pending_execution_manager.get_pending_executions()
        
        if not pending_executions:
            logger.info("No pending trades to execute")
            return
        
        executed_count = 0
        
        for execution in pending_executions:
            try:
                # Get current price
                current_price = await self.real_time_manager.get_current_price(execution.ticker)
                if not current_price:
                    logger.warning(f"Could not get current price for {execution.ticker}")
                    continue
                
                # Check if price has moved significantly
                price_change = abs(current_price - execution.estimated_price) / execution.estimated_price
                if price_change > 0.05:  # 5% price change
                    logger.warning(f"Price changed significantly for {execution.ticker}: "
                                 f"estimated ${execution.estimated_price:.2f}, current ${current_price:.2f}")
                    continue
                
                # Place order
                api = self.alpaca_manager.api
                if api:
                    if execution.signal_type == TradeSignalType.BUY:
                        order_side = 'buy'
                    else:  # SELL
                        order_side = 'sell'
                    
                    api.submit_order(
                        symbol=execution.ticker,
                        qty=execution.quantity,
                        side=order_side,
                        type='limit',
                        limit_price=current_price,
                        time_in_force='day'
                    )
                    
                    logger.info(f"Executed {order_side} order for {execution.ticker}: "
                              f"{execution.quantity} shares at ${current_price:.2f}")
                    
                    # Set stop loss
                    stop_loss = await self.stop_loss_manager.calculate_initial_stop_loss(
                        execution.ticker, current_price, execution.signal_type
                    )
                    self.stop_loss_manager.active_stops[execution.ticker] = stop_loss
                    
                    # Start monitoring
                    if execution.ticker not in self.active_monitoring:
                        self.active_monitoring[execution.ticker] = asyncio.create_task(
                            self._monitor_single_position(execution.ticker)
                        )
                    
                    # Remove from pending
                    await self.pending_execution_manager.clear_executed(execution.ticker)
                    executed_count += 1
                
                # Small delay between orders
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Error executing trade for {execution.ticker}: {e}")
        
        logger.info(f"Executed {executed_count} pending trades")
    
    async def run_scheduled_scan(self):
        """Run scheduled position scan based on volatility"""
        volatility_state = await self.real_time_manager.monitor_volatility()
        scan_interval = self.volatility_monitor.get_scan_interval()
        
        logger.info(f"Volatility: {volatility_state.value}, Scan interval: {scan_interval} minutes")
        
        # Scan existing positions for reevaluation
        await self._scan_existing_positions()
        
        # Update last scan time
        self.last_scan_time = datetime.now()
    
    async def _scan_existing_positions(self):
        """Scan existing positions for potential exits"""
        positions = await self.alpaca_manager.get_positions()
        
        for position in positions:
            ticker = position['symbol']
            try:
                # Get current ranking for the ticker
                rankings = await self.scanner.combined_ranking_engine.rank_all_tickers_optimized(100)
                current_ranking = next((r for r in rankings if r.ticker == ticker), None)
                
                if current_ranking:
                    # Calculate P&L
                    entry_price = float(position['avg_entry_price'])
                    current_price = float(position['current_price'])
                    if float(position['quantity']) > 0:  # LONG
                        pnl_pct = (current_price - entry_price) / entry_price
                    else:  # SHORT
                        pnl_pct = (entry_price - current_price) / entry_price
                    
                    # Evaluate trade score
                    trade_score = await self.exit_manager.evaluate_trade_score(
                        ticker, current_ranking, position
                    )
                    
                    # Check if should exit
                    should_exit, reason = await self.exit_manager.should_exit_trade(
                        ticker, trade_score, pnl_pct
                    )
                    
                    if should_exit:
                        logger.info(f"Scheduled scan exit signal for {ticker}: {reason}")
                        await self._exit_position(ticker, reason)
                        
            except Exception as e:
                logger.error(f"Error scanning position {ticker}: {e}")

# ======================== UNIFIED DISPLAY MANAGER ======================== #
class UnifiedDisplayManager:
    """Unified display manager to combine all output into one organized group"""
    
    def __init__(self):
        self.section_width = 80
        self.section_char = "="
        self.subsection_char = "-"
        self.display_groups = {
            'capital_detection': True,  # NEW: Capital detection status
            'system_status': True,
            'task_scheduler': True,
            'ranking_results': False,  # Disabled by default for cleaner output
            'performance': False,      # Disabled by default
            'trading_signals': False,   # Disabled by default
            'trading_status': True     # Trading status enabled
        }
    
    def set_display_group(self, group_name: str, enabled: bool):
        """Enable/disable specific display groups"""
        if group_name in self.display_groups:
            self.display_groups[group_name] = enabled
    
    def display_section_header(self, title: str):
        """Display a section header"""
        print(f"\n{self.section_char * self.section_width}")
        print(f"{title.center(self.section_width)}")
        print(f"{self.section_char * self.section_width}")
    
    def display_subsection(self, title: str):
        """Display a subsection header"""
        print(f"\n{self.subsection_char * self.section_width}")
        print(f"{title}")
        print(f"{self.subsection_char * self.section_width}")
    
    def display_capital_detection_status(self, capital_detector):
        """Display capital and position detection status"""
        if not self.display_groups['capital_detection']:
            return
            
        self.display_section_header("CAPITAL & POSITION DETECTION STATUS")
        
        if not capital_detector.detection_complete:
            print("⏳ Capital and position detection in progress...")
            return
        
        # Capital information
        capital_info = capital_detector.capital_info
        if capital_info:
            print(f"💰 Portfolio Value: ${capital_info.get('portfolio_value', 0):.2f}")
            print(f"💵 Available Cash: ${capital_info.get('cash', 0):.2f}")
            print(f"💳 Buying Power: ${capital_info.get('buying_power', 0):.2f}")
            print(f"📈 Equity: ${capital_info.get('equity', 0):.2f}")
            print(f"🔒 Status: {capital_info.get('status', 'N/A')}")
        else:
            print("❌ No capital information available")
        
        # Positions information
        positions = capital_detector.positions_info
        print(f"\n📊 Existing Positions: {len(positions) if positions else 0}")
        if positions:
            for pos in positions[:5]:  # Show top 5 positions
                pl_percent = pos.get('unrealized_plpc', 0) * 100
                pl_emoji = "🟢" if pos.get('unrealized_pl', 0) >= 0 else "🔴"
                print(f"   {pos['symbol']}: {pos['quantity']:.0f} shares | P/L: {pl_emoji} ${pos['unrealized_pl']:.2f} ({pl_percent:.2f}%)")
            if len(positions) > 5:
                print(f"   ... and {len(positions) - 5} more positions")
        else:
            print("   No active positions")
        
        # Portfolio allocation
        portfolio_summary = capital_detector.portfolio_summary
        if portfolio_summary:
            print(f"\n📋 Allocation:")
            print(f"   💰 Cash: {portfolio_summary.get('cash_allocation_pct', 0):.1f}%")
            print(f"   📈 Positions: {portfolio_summary.get('positions_allocation_pct', 0):.1f}%")
    
    def display_task_scheduler_status(self, task_scheduler):
        """Display task scheduler status"""
        if not self.display_groups['task_scheduler']:
            return
            
        self.display_section_header("SCHEDULED TASK STATUS")
        
        for task_name, task_info in task_scheduler.tasks.items():
            status = task_info['status']
            next_run = task_info['next_run']
            
            if next_run:
                wait_time = task_scheduler.format_wait_time(next_run)
                next_run_str = next_run.strftime("%Y-%m-%d %H:%M:%S")
            else:
                wait_time = "N/A"
                next_run_str = "N/A"
            
            # Status emoji - UPDATED TO MATCH REQUESTED FORMAT
            if status == 'completed':
                status_emoji = "✅"
            elif status == 'running':
                status_emoji = "🟡"
            elif status == 'pending':
                status_emoji = "⏳"
            elif status == 'failed':
                status_emoji = "❌"
            else:
                status_emoji = "⚙️"
            
            print(f"{task_name:20s} | {status_emoji} {status:10s} | Next: {next_run_str} | Wait: {wait_time}")
    
    def display_ranking_results(self, rankings, max_display=20):
        """Display ranking results in a formatted table"""
        if not self.display_groups['ranking_results'] or not rankings:
            return
            
        self.display_section_header(f"TOP {min(max_display, len(rankings))} RANKED TICKERS")
        
        # Header
        print(f"{'Rank':<4} {'Ticker':<8} {'Score':<6} {'Conf':<5} {'Signal':<8} {'Price':<8}")
        print(f"{'-'*4} {'-'*8} {'-'*6} {'-'*5} {'-'*8} {'-'*8}")
        
        # Display top rankings
        for i, ranking in enumerate(rankings[:max_display]):
            # Signal emoji
            signal_emoji = "🟢" if ranking.signal_type == "LONG" else "🔴" if ranking.signal_type == "SHORT" else "⚪"
            
            print(f"{ranking.rank:<4} {ranking.ticker:<8} {ranking.total_score:.3f} {ranking.confidence:.2f} {signal_emoji} {ranking.signal_type:<6} ${ranking.current_price:.2f}")
        
        # Signal summary
        if len(rankings) > max_display:
            print(f"\n... and {len(rankings) - max_display} more tickers")
        
        signal_counts = {}
        for ranking in rankings:
            signal_type = ranking.signal_type
            signal_counts[signal_type] = signal_counts.get(signal_type, 0) + 1
        
        print(f"\n📊 Signal Summary: LONG: {signal_counts.get('LONG', 0)} | SHORT: {signal_counts.get('SHORT', 0)} | NEUTRAL: {signal_counts.get('NEUTRAL', 0)}")
    
    def display_performance_metrics(self, performance_metrics):
        """Display performance metrics"""
        if not self.display_groups['performance'] or not performance_metrics:
            return
            
        self.display_section_header("PERFORMANCE METRICS")
        
        print(f"{'Function':<30} {'Calls':<8} {'Avg Time':<10} {'Total Time':<12}")
        print(f"{'-'*30} {'-'*8} {'-'*10} {'-'*12}")
        
        for func_name, metrics in performance_metrics.items():
            count = metrics.get('count', 0)
            total_duration = metrics.get('total_duration', 0)
            avg_time = total_duration / count if count > 0 else 0
            
            if count > 0:
                print(f"{func_name:<30} {count:<8} {avg_time:.3f}s     {total_duration:.2f}s")
    
    def display_trading_signals(self, trade_signals):
        """Display trading signals"""
        if not self.display_groups['trading_signals'] or not trade_signals:
            return
            
        self.display_section_header("TRADING SIGNALS")
        
        for signal in trade_signals:
            signal_emoji = "🟢 BUY" if signal.signal_type == TradeSignalType.BUY else "🔴 SELL" if signal.signal_type == TradeSignalType.SELL else "⚪ HOLD"
            print(f"{signal_emoji} {signal.ticker:<8} | Qty: {signal.quantity:<4} | Price: ${signal.price:.2f} | Conf: {signal.confidence:.2f}")
            print(f"   Reason: {signal.reason}")
            if signal != trade_signals[-1]:  # Don't print separator after last signal
                print(f"   {'.'*50}")
    
    def display_trading_status(self, trading_manager):
        """Display trading account status"""
        if not self.display_groups['trading_status']:
            return
            
        self.display_section_header("TRADING ACCOUNT STATUS")
        
        try:
            # Run in event loop if we're in async context
            import asyncio
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # We're in async context, create task
                    asyncio.create_task(self._async_display_trading_status(trading_manager))
                    return
            except:
                pass
            
            # Synchronous fallback
            asyncio.run(self._async_display_trading_status(trading_manager))
        except Exception as e:
            print(f"Error displaying trading status: {e}")
    
    async def _async_display_trading_status(self, trading_manager):
        """Async helper to display trading status"""
        try:
            account_info = await trading_manager.get_account_info()
            positions = await trading_manager.get_positions()
            portfolio_summary = await trading_manager.get_portfolio_summary()
            
            if not account_info:
                print("❌ Unable to fetch account information")
                return
            
            # Account basics
            print(f"💰 Portfolio Value: ${account_info.get('portfolio_value', 0):.2f}")
            print(f"💵 Available Cash: ${account_info.get('cash', 0):.2f}")
            print(f"💳 Buying Power: ${account_info.get('buying_power', 0):.2f}")
            print(f"📈 Equity: ${account_info.get('equity', 0):.2f}")
            print(f"🔒 Status: {account_info.get('status', 'N/A')} | Trading Blocked: {account_info.get('trading_blocked', 'N/A')}")
            
            # Positions
            print(f"\n📊 Positions: {len(positions)}")
            if positions:
                for pos in positions[:5]:  # Show top 5 positions
                    pl_percent = pos.get('unrealized_plpc', 0) * 100
                    pl_emoji = "🟢" if pos.get('unrealized_pl', 0) >= 0 else "🔴"
                    print(f"   {pos['symbol']}: {pos['quantity']:.0f} shares | P/L: {pl_emoji} ${pos['unrealized_pl']:.2f} ({pl_percent:.2f}%)")
                if len(positions) > 5:
                    print(f"   ... and {len(positions) - 5} more positions")
            else:
                print("   No active positions")
            
            # Portfolio allocation
            if portfolio_summary:
                print(f"\n📋 Allocation:")
                print(f"   💰 Cash: {portfolio_summary.get('cash_allocation_pct', 0):.1f}%")
                print(f"   📈 Positions: {portfolio_summary.get('positions_allocation_pct', 0):.1f}%")
            
        except Exception as e:
            print(f"Error in trading status display: {e}")
    
    def display_trade_execution_status(self, trade_executor):
        """Display trade execution system status"""
        if not self.display_groups['trading_status']:
            return
            
        self.display_section_header("TRADE EXECUTION STATUS")
        
        try:
            # Display pending executions
            pending = asyncio.run(trade_executor.pending_execution_manager.get_pending_executions())
            print(f"📋 Pending Executions: {len(pending)}")
            
            if pending:
                for i, exec in enumerate(pending[:3], 1):
                    signal_emoji = "🟢" if exec.signal_type == TradeSignalType.BUY else "🔴"
                    print(f"   {i}. {signal_emoji} {exec.ticker}: {exec.quantity} shares "
                          f"@ ${exec.estimated_price:.2f} (Conf: {exec.confidence:.2f})")
                if len(pending) > 3:
                    print(f"   ... and {len(pending) - 3} more")
            else:
                print("   No pending executions")
            
            # Display active stops
            active_stops = trade_executor.stop_loss_manager.active_stops
            print(f"🛡️  Active Stop Losses: {len(active_stops)}")
            
            if active_stops:
                for ticker, stop in list(active_stops.items())[:3]:
                    print(f"   {ticker}: ${stop.current_stop_price:.2f} ({stop.stop_type})")
                if len(active_stops) > 3:
                    print(f"   ... and {len(active_stops) - 3} more")
            else:
                print("   No active stop losses")
            
            # Display monitoring status
            monitoring = trade_executor.active_monitoring
            print(f"🔍 Active Monitoring: {len(monitoring)} positions")
            
            # Display volatility state
            volatility_state = trade_executor.volatility_monitor.current_state
            scan_interval = trade_executor.volatility_monitor.get_scan_interval()
            print(f"📊 Volatility: {volatility_state.value} (Scan every {scan_interval}min)")
            
        except Exception as e:
            print(f"Error displaying trade execution status: {e}")
    
    def display_comprehensive_status(self, scanner, db_manager, task_scheduler, 
                                   rankings=None, performance_metrics=None, trade_signals=None):
        """Display comprehensive status report combining all components"""
        # Display current time at the top
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"🕒 Current Time: {current_time}")
        
        # Display all enabled sections - CAPITAL DETECTION FIRST
        if hasattr(scanner, 'capital_detector'):
            self.display_capital_detection_status(scanner.capital_detector)
        
        self.display_task_scheduler_status(task_scheduler)
        
        # Display trade execution status if available
        if hasattr(scanner, 'trade_executor'):
            self.display_trade_execution_status(scanner.trade_executor)
        else:
            self.display_trading_status(scanner.alpaca_manager)

# Initialize global display manager
display_manager = UnifiedDisplayManager()

# ======================== RUN STATUS MANAGER ======================== #
class RunStatusManager:
    """Manage run status to track sector detection completion and avoid redundant scans"""
    
    def __init__(self, status_file: str = None):
        self.status_file = status_file or config.RUN_STATUS_FILE
        self.status_data = self._load_status()
    
    def _load_status(self) -> Dict[str, Any]:
        """Load run status from JSON file"""
        default_status = {
            "last_sector_completion_date": None,
            "last_full_cycle_completed": None,
            "sector_cache_valid": False,
            "last_run_timestamp": None
        }
        
        try:
            if os.path.exists(self.status_file):
                with open(self.status_file, 'r') as f:
                    return json.load(f)
            else:
                return default_status
        except Exception as e:
            logger.warning(f"Failed to load run status: {e}, using default")
            return default_status
    
    def save_status(self):
        """Save current status to file"""
        try:
            self.status_data["last_run_timestamp"] = datetime.now().isoformat()
            with open(self.status_file, 'w') as f:
                json.dump(self.status_data, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save run status: {e}")
    
    def should_run_sector_detection(self) -> bool:
        """Check if sector detection should run based on completion status"""
        if not self.status_data.get("sector_cache_valid", False):
            return True
        
        last_completion_date = self.status_data.get("last_sector_completion_date")
        if not last_completion_date:
            return True
        
        try:
            # Parse the last completion date
            last_date = datetime.fromisoformat(last_completion_date).date()
            today = datetime.now().date()
            
            # If we completed today and it's before 23:59, don't run again
            if last_date == today:
                current_time = datetime.now().time()
                cutoff_time = datetime.strptime("23:59", "%H:%M").time()
                
                if current_time < cutoff_time:
                    logger.info("Sector detection already completed today, using cached data")
                    return False
            
            return True
            
        except Exception as e:
            logger.warning(f"Error checking sector detection status: {e}")
            return True
    
    def mark_sector_detection_completed(self):
        """Mark sector detection as completed for today"""
        self.status_data["last_sector_completion_date"] = datetime.now().isoformat()
        self.status_data["sector_cache_valid"] = True
        self.status_data["last_full_cycle_completed"] = datetime.now().isoformat()
        self.save_status()
        logger.info("Sector detection marked as completed for today")
    
    def mark_sector_cache_invalid(self):
        """Mark sector cache as invalid (force refresh next run)"""
        self.status_data["sector_cache_valid"] = False
        self.save_status()

# ======================== SECTOR FALLBACK ======================== #
try:
    import yfinance as yf
except ImportError:
    yf = None
    logger.warning("yfinance not available for fallback sector data")

# ======================== TASK SCHEDULER STATUS ======================== #
class TaskSchedulerStatus:
    """Display scheduled task status with next run times"""
    
    def __init__(self):
        self.tasks = {
            'CAPITAL_DETECTION': {  # NEW: Capital detection task
                'status': 'pending',
                'next_run': None,
                'schedule_time': self._calculate_capital_detection_time(),
                'last_run': None
            },
            'TICKER_REFRESH': {
                'status': 'pending',
                'next_run': None,
                'schedule_time': config.SCAN_TIME,
                'last_run': None
            },
            'SECTOR_DATA': {
                'status': 'pending', 
                'next_run': None,
                'schedule_time': self._calculate_sector_time(),
                'last_run': None
            },
            'RANKING_ANALYSIS': {
                'status': 'pending',
                'next_run': None,
                'schedule_time': self._calculate_ranking_time(),
                'last_run': None
            },
            'TRADE_EXECUTION': {
                'status': 'pending',
                'next_run': None,
                'schedule_time': config.TRADE_EXECUTION_TIME,
                'last_run': None
            }
        }
        # Initialize next run times
        for task_name in self.tasks:
            self._calculate_next_run(task_name)
        
    def _calculate_capital_detection_time(self):
        """Calculate capital detection time (5 minutes before ticker refresh)"""
        scan_time = datetime.strptime(config.SCAN_TIME, "%H:%M")
        capital_time = (scan_time - timedelta(minutes=5)).strftime("%H:%M")
        return capital_time
    
    def _calculate_sector_time(self):
        """Calculate sector data time (5 minutes after ticker refresh)"""
        scan_time = datetime.strptime(config.SCAN_TIME, "%H:%M")
        sector_time = (scan_time + timedelta(minutes=5)).strftime("%H:%M")
        return sector_time
    
    def _calculate_ranking_time(self):
        """Calculate ranking analysis time (15 minutes after ticker refresh)"""
        scan_time = datetime.strptime(config.SCAN_TIME, "%H:%M")
        ranking_time = (scan_time + timedelta(minutes=15)).strftime("%H:%M")
        return ranking_time
    
    def update_task_status(self, task_name: str, status: str, last_run: datetime = None):
        """Update task status and calculate next run time"""
        if task_name in self.tasks:
            self.tasks[task_name]['status'] = status
            if last_run:
                self.tasks[task_name]['last_run'] = last_run
            self._calculate_next_run(task_name)
    
    def _calculate_next_run(self, task_name: str):
        """Calculate next run time for a task"""
        task = self.tasks[task_name]
        now = datetime.now()
        
        # Parse scheduled time
        scheduled_time = datetime.strptime(task['schedule_time'], "%H:%M").time()
        
        # Create datetime for today with scheduled time
        next_run = datetime.combine(now.date(), scheduled_time)
        
        # If scheduled time has passed today, schedule for tomorrow
        if next_run <= now:
            next_run += timedelta(days=1)
        
        task['next_run'] = next_run
    
    def format_wait_time(self, next_run: datetime) -> str:
        """Format wait time in hours and minutes"""
        now = datetime.now()
        wait_seconds = (next_run - now).total_seconds()
        
        if wait_seconds < 0:
            return "0h  0m"
        
        hours = int(wait_seconds // 3600)
        minutes = int((wait_seconds % 3600) // 60)
        
        return f"{hours:2d}h {minutes:2d}m"
    
    def display_status(self):
        """Display current task status in the requested format"""
        display_manager.display_task_scheduler_status(self)
    
    def mark_all_completed(self):
        """Mark all tasks as completed and calculate next runs"""
        now = datetime.now()
        for task_name in self.tasks:
            self.update_task_status(task_name, 'completed', now)

# Initialize global task scheduler status
task_scheduler = TaskSchedulerStatus()

# ======================== DECORATORS ======================== #
def handle_errors(max_retries=3, retry_delay=1):
    """Unified error handling decorator for both sync and async methods"""
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            return await _error_handler_impl(func, max_retries, retry_delay, *args, **kwargs)
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            return _error_handler_impl(func, max_retries, retry_delay, *args, **kwargs)
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator

async def _error_handler_impl_async(func, max_retries, retry_delay, *args, **kwargs):
    """Unified async error handling implementation"""
    retries = 0
    while retries <= max_retries:
        try:
            return await func(*args, **kwargs)
        except (APIError, DatabaseError) as e:
            retries += 1
            if retries > max_retries:
                logger.error(f"Max retries exceeded for {func.__name__}: {e}")
                raise
            logger.warning(f"Retry {retries}/{max_retries} for {func.__name__} after error: {e}")
            await asyncio.sleep(retry_delay * retries)
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {e}")
            raise
    return None

def _error_handler_impl_sync(func, max_retries, retry_delay, *args, **kwargs):
    """Unified sync error handling implementation"""
    retries = 0
    while retries <= max_retries:
        try:
            return func(*args, **kwargs)
        except (APIError, DatabaseError) as e:
            retries += 1
            if retries > max_retries:
                logger.error(f"Max retries exceeded for {func.__name__}: {e}")
                raise
            logger.warning(f"Retry {retries}/{max_retries} for {func.__name__} after error: {e}")
            time.sleep(retry_delay * retries)
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {e}")
            raise
    return None

def _error_handler_impl(func, max_retries, retry_delay, *args, **kwargs):
    """Router for error handling implementation"""
    if asyncio.iscoroutinefunction(func):
        return _error_handler_impl_async(func, max_retries, retry_delay, *args, **kwargs)
    else:
        return _error_handler_impl_sync(func, max_retries, retry_delay, *args, **kwargs)

def monitor_performance(func):
    """Unified performance monitoring decorator - SILENT VERSION"""
    @wraps(func)
    async def async_wrapper(self, *args, **kwargs):
        start_time = time.time()
        try:
            result = await func(self, *args, **kwargs)
            return result
        finally:
            duration = time.time() - start_time
            # Only log if operation takes significant time
            if duration > 1.0:  # Only log operations over 1 second
                logger.debug(f"{func.__name__} executed in {duration:.2f}s")  # Changed to debug

    @wraps(func)
    def sync_wrapper(self, *args, **kwargs):
        start_time = time.time()
        try:
            result = func(self, *args, **kwargs)
            return result
        finally:
            duration = time.time() - start_time
            if duration > 1.0:
                logger.debug(f"{func.__name__} executed in {duration:.2f}s")  # Changed to debug
    
    return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

def _track_performance_metrics(self, func_name: str, duration: float):
    """Track performance metrics in a unified way"""
    if hasattr(self, 'performance_metrics'):
        if func_name not in self.performance_metrics:
            self.performance_metrics[func_name] = {
                'total_duration': 0,
                'count': 0,
                'last_execution': 0
            }
        
        self.performance_metrics[func_name]['total_duration'] += duration
        self.performance_metrics[func_name]['count'] += 1
        self.performance_metrics[func_name]['last_execution'] = time.time()

# ======================== SIMPLIFIED DATABASE MANAGER ======================== #
class DatabaseManager:
    """Simplified database manager that only handles active_tickers in the ticker table"""
    
    def __init__(self):
        self.pool = None
        self._init_lock = asyncio.Lock()
        self.performance_metrics = {}
    
    async def initialize(self):
        """Initialize the connection pool asynchronously"""
        async with self._init_lock:
            if self.pool is None:
                try:
                    self.pool = await asyncpg.create_pool(
                        min_size=3,
                        max_size=20,
                        host=config.POSTGRES_HOST,
                        port=config.POSTGRES_PORT,
                        database=config.POSTGRES_DB,
                        user=config.POSTGRES_USER,
                        password=config.POSTGRES_PASSWORD
                    )
                    await self._init_database()
                    logger.info("✅ Database has been initialized and loaded")
                except Exception as e:
                    logger.error(f"Database connection failed: {e}")
                    raise DatabaseError(f"Database connection failed: {e}")

    @contextlib.asynccontextmanager
    async def get_connection(self):
        """Get a connection from the pool with unified error handling"""
        if self.pool is None:
            await self.initialize()
            
        conn = None
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                conn = await self.pool.acquire()
                await conn.execute("SELECT 1")  # Validate connection
                yield conn
                break
            except (asyncpg.PostgresConnectionError, asyncpg.InterfaceError) as e:
                logger.warning(f"Database connection error (attempt {retry_count+1}): {e}")
                if conn:
                    try:
                        await self.pool.release(conn)
                    except:
                        pass
                retry_count += 1
                if retry_count >= max_retries:
                    raise DatabaseError(f"Failed to get valid connection after {max_retries} attempts")
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Database error: {e}")
                if conn:
                    try:
                        await self.pool.release(conn)
                    except:
                        pass
                raise DatabaseError(f"Database error: {e}")
            finally:
                if conn and not conn.is_closed() and retry_count < max_retries:
                    try:
                        await self.pool.release(conn)
                    except Exception as e:
                        logger.error(f"Error returning connection to pool: {e}")

    async def close_all_connections(self):
        """Close all connections in the pool"""
        if self.pool:
            await self.pool.close()

    def _convert_numpy_types(self, params):
        """Convert numpy data types to native Python types for database compatibility"""
        return tuple(
            int(param) if isinstance(param, np.integer) else
            float(param) if isinstance(param, np.floating) else param
            for param in params
        )

    @monitor_performance
    @handle_errors()
    async def execute_query(self, query: str, params: tuple = ()) -> List[Dict]:
        """Unified query execution"""
        async with self.get_connection() as conn:
            records = await conn.fetch(query, *params)
            return [dict(record) for record in records]
            
    @monitor_performance
    @handle_errors()
    async def execute_write(self, query: str, params: tuple = ()) -> int:
        """Unified write execution"""
        converted_params = self._convert_numpy_types(params)
        async with self.get_connection() as conn:
            result = await conn.execute(query, *converted_params)
            return _parse_write_result(result)

    async def _init_database(self):
        """Initialize database tables - simplified to only create tickers table"""
        async with self.get_connection() as conn:
            # Create only the tickers table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS tickers (
                    ticker TEXT PRIMARY KEY,
                    name TEXT,
                    primary_exchange TEXT,
                    last_updated_utc TEXT,
                    type TEXT,
                    market TEXT,
                    locale TEXT,
                    currency_name TEXT,
                    active INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create index only for active tickers
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_tickers_active ON tickers(active) WHERE active = 1')
            
            logger.info("Database tables initialized successfully")

    @monitor_performance
    @handle_errors()
    async def upsert_tickers(self, tickers: List[Dict]) -> Tuple[int, int]:
        """Upsert tickers into the database - simplified version"""
        if not tickers:
            return 0, 0
            
        # Prepare arrays for bulk operation
        tickers_arr = []
        names_arr = []
        primary_exchange_arr = []
        last_updated_utc_arr = []
        type_arr = []
        market_arr = []
        locale_arr = []
        currency_name_arr = []
        active_arr = []
        
        for t in tickers:
            tickers_arr.append(t['ticker'])
            names_arr.append(t.get('name'))
            primary_exchange_arr.append(t.get('primary_exchange'))
            last_updated_utc_arr.append(t.get('last_updated_utc'))
            type_arr.append(t.get('type'))
            market_arr.append(t.get('market'))
            locale_arr.append(t.get('locale'))
            currency_name_arr.append(t.get('currency_name'))
            active_arr.append(1)
        
        inserted = 0
        updated = 0
        
        async with self.get_connection() as conn:
            # Use transaction for better performance
            async with conn.transaction():
                try:
                    # Bulk upsert using UNNEST
                    result = await conn.fetchrow('''
                        WITH input_data AS (
                            SELECT 
                                unnest($1::text[]) AS ticker,
                                unnest($2::text[]) AS name,
                                unnest($3::text[]) AS primary_exchange,
                                unnest($4::text[]) AS last_updated_utc,
                                unnest($5::text[]) AS type,
                                unnest($6::text[]) AS market,
                                unnest($7::text[]) AS locale,
                                unnest($8::text[]) AS currency_name,
                                unnest($9::int[]) AS active
                        ),
                        updated AS (
                            UPDATE tickers t
                            SET 
                                name = i.name,
                                primary_exchange = i.primary_exchange,
                                last_updated_utc = i.last_updated_utc,
                                type = i.type,
                                market = i.market,
                                locale = i.locale,
                                currency_name = i.currency_name,
                                active = i.active,
                                updated_at = CURRENT_TIMESTAMP
                            FROM input_data i
                            WHERE t.ticker = i.ticker
                            RETURNING t.ticker
                        ),
                        inserted AS (
                            INSERT INTO tickers 
                                (ticker, name, primary_exchange, last_updated_utc, 
                                 type, market, locale, currency_name, active)
                            SELECT 
                                i.ticker, i.name, i.primary_exchange, i.last_updated_utc,
                                i.type, i.market, i.locale, i.currency_name, i.active
                            FROM input_data i
                            WHERE i.ticker NOT IN (SELECT ticker FROM updated)
                            RETURNING ticker
                        )
                        SELECT 
                            (SELECT COUNT(*) FROM inserted) AS inserted_count,
                            (SELECT COUNT(*) FROM updated) AS updated_count
                    ''', tickers_arr, names_arr, primary_exchange_arr, last_updated_utc_arr,
                    type_arr, market_arr, locale_arr, currency_name_arr, active_arr)
                    
                    inserted = result['inserted_count'] if result else 0
                    updated = result['updated_count'] if result else 0
                    
                except Exception as e:
                    logger.error(f"Transaction failed during ticker upsert: {e}")
                    raise DatabaseError(f"Transaction failed during ticker upsert: {e}")
            
        return inserted, updated

    @handle_errors()
    async def get_all_active_tickers(self) -> List[Dict]:
        """Get all active tickers from the database - main method we need"""
        return await self.execute_query(
            "SELECT * FROM tickers WHERE active = 1 ORDER BY ticker"
        )

    @monitor_performance
    @handle_errors()
    async def mark_tickers_inactive(self, tickers: List[str]) -> int:
        """Mark tickers as inactive using bulk operations"""
        if not tickers:
            return 0
            
        marked = 0
        
        async with self.get_connection() as conn:
            # Bulk update to mark as inactive
            update_result = await conn.execute(
                "UPDATE tickers SET active = 0, updated_at = CURRENT_TIMESTAMP WHERE ticker = ANY($1)",
                tickers
            )
            
            marked = int(update_result.split()[-1]) if "UPDATE" in update_result else 0
                
        return marked

def _parse_write_result(result: str) -> int:
    """Parse database write result to get row count"""
    if "INSERT" in result or "UPDATE" in result or "DELETE" in result:
        return int(result.split()[-1])
    return 0

# ======================== STRATEGY CORE ======================== #
class SignalType(Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    NEUTRAL = "NEUTRAL"

class TradeSignalType(Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"

@dataclass
class TickerRanking:
    """Unified ticker ranking data class"""
    ticker: str
    name: str
    primary_exchange: str
    total_score: float
    trend_score: float
    mean_reversion_score: float
    volume_score: float
    volatility_score: float
    momentum_score: float
    confidence: float
    trend_strength: float
    mean_reversion_strength: float
    volume_confirmation: float
    signal_type: str
    current_price: float
    rank: int = 0
    timestamp: str = None

@dataclass
class TradeSignal:
    """Trade signal with enhanced information"""
    ticker: str
    signal_type: TradeSignalType
    quantity: int
    price: float
    confidence: float
    ranking: TickerRanking
    reason: str
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# ======================== ENHANCED DATA PROVIDER ======================== #
class DataProvider:
    """Optimized data provider with connection pooling"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.client = RESTClient(api_key)
        self.rate_limit_delay = config.RATE_LIMIT_DELAY
        self._session_pool = []
        self._session_lock = asyncio.Lock()
        
    @handle_errors(max_retries=3, retry_delay=1)
    async def get_historical_bars(self, symbol: str, timeframe: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Get historical bar data from Polygon with minimal logging"""
        return await self.get_historical_bars_optimized(symbol, timeframe, start_date, end_date)
        
    async def get_historical_bars_optimized(self, symbol: str, timeframe: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Optimized data fetching with memory efficiency"""
        timeframe_map = {'1min': 'minute', '5min': 'minute', '15min': 'minute', 
                        '1h': 'hour', '1day': 'day'}
        timespan = timeframe_map.get(timeframe, 'day')
        multiplier = 1
        
        if timeframe == '5min':
            multiplier = 5
        elif timeframe == '15min':
            multiplier = 15
            
        try:
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor(max_workers=2) as executor:
                aggs = await loop.run_in_executor(
                    executor, 
                    lambda: list(self.client.get_aggs(
                        symbol, multiplier, timespan, start_date, end_date, limit=50000
                    ))
                )
            
            if not aggs:
                return pd.DataFrame()
                
            # Use list comprehension for faster DataFrame creation
            data = [{
                'timestamp': pd.to_datetime(agg.timestamp, unit='ms'),
                'open': float(agg.open),
                'high': float(agg.high), 
                'low': float(agg.low),
                'close': float(agg.close),
                'volume': int(agg.volume)
            } for agg in aggs]
            
            df = pd.DataFrame(data)
            
            if df.empty:
                return df
                
            df.set_index('timestamp', inplace=True)
            df.sort_index(inplace=True)
            
            # Optimize memory usage
            if config.MEMORY_EFFICIENT:
                for col in ['open', 'high', 'low', 'close']:
                    df[col] = pd.to_numeric(df[col], downcast='float')
                df['volume'] = pd.to_numeric(df['volume'], downcast='unsigned')
            
            return df
            
        except Exception as e:
            logger.debug(f"Optimized data fetch failed for {symbol}: {e}")
            return pd.DataFrame()

    async def get_historical_bars_ultrafast(self, symbol: str, timeframe: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Ultra-fast data fetching with minimal overhead"""
        try:
            timeframe_map = {'1min': 'minute', '5min': 'minute', '15min': 'minute', 
                            '1h': 'hour', '1day': 'day'}
            timespan = timeframe_map.get(timeframe, 'day')
            multiplier = 1
            
            if timeframe == '5min':
                multiplier = 5
            elif timeframe == '15min':
                multiplier = 15
                
            # Fast synchronous call with minimal processing
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor(max_workers=1) as executor:
                aggs = await loop.run_in_executor(
                    executor, 
                    lambda: list(self.client.get_aggs(
                        symbol, multiplier, timespan, start_date, end_date, limit=50000
                    ))
                )
            
            if not aggs:
                return pd.DataFrame()
                
            # Ultra-fast DataFrame creation
            data = []
            for agg in aggs:
                data.append([
                    pd.to_datetime(agg.timestamp, unit='ms'),
                    float(agg.open),
                    float(agg.high), 
                    float(agg.low),
                    float(agg.close),
                    int(agg.volume)
                ])
            
            df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df.set_index('timestamp', inplace=True)
            df.sort_index(inplace=True)
            
            # Aggressive memory optimization
            if config.AGGRESSIVE_MEMORY_OPTIMIZATION:
                for col in ['open', 'high', 'low', 'close']:
                    df[col] = df[col].astype(np.float32)
                df['volume'] = df['volume'].astype(np.uint32)
            
            return df
            
        except Exception:
            return pd.DataFrame()

    def _optimize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggressive DataFrame optimization"""
        # Convert to efficient data types
        for col in ['open', 'high', 'low', 'close']:
            df[col] = df[col].astype(np.float32)
        df['volume'] = df['volume'].astype(np.uint32)
        
        return df

# ======================== ENHANCED MOMENTUM MEAN REVERSION STRATEGY ======================== #
class MomentumMeanReversionStrategy:
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or self._get_default_config()
        self.performance_metrics = {}

    def _get_default_config(self) -> Dict[str, Any]:
        return {
            'trend_ema_fast': 13,
            'trend_ema_slow': 34,
            'trend_adx_period': 21,
            'min_trend_strength': 25,
            'mean_reversion_rsi_period': 21,
            'rsi_oversold': 25,
            'rsi_overbought': 75,
            'bollinger_period': 26,
            'bollinger_std': 2.2,
            'momentum_period': 14,
            'min_momentum_strength': 0.03,
            'volume_spike_threshold': 2.0,
            'atr_period': 21,
            'volume_ma_period': 30,
            'min_volume_ratio': 1.5,
            'min_signal_score': 0.4,
            'strong_signal_threshold': 0.6,
            'confidence_threshold': 0.5,
            
            # ENHANCED WEIGHTS: Increased momentum from 10% to 17%
            'weight_trend_strength': 0.28,      # Reduced from 0.30
            'weight_mean_reversion': 0.25,      # Kept same
            'weight_volume': 0.25,              # Kept same  
            'weight_momentum': 0.17,            # Increased from 0.10 to 0.17
            'weight_volatility': 0.05,          # Reduced from 0.10 to 0.05
            
            'min_avg_volume': 500000,
            'max_daily_volatility': 0.12,
            
            # NEW: Momentum confirmation parameters
            'momentum_confirmation_enabled': True,
            'price_breakout_lookback': 20,
            'volume_surge_threshold': 2.5,
            'breakout_confirmation_weight': 0.3,
        }

    def _validate_timeframe_alignment(self, df: pd.DataFrame) -> bool:
        """Validate that all indicators are working on compatible timeframes"""
        min_bars_required = max(
            self.config['trend_ema_slow'],
            self.config['bollinger_period'], 
            self.config['atr_period'],
            self.config['momentum_period'],
            self.config.get('price_breakout_lookback', 20),
            20  # Minimum bars for reliable indicators
        )
        
        if len(df) < min_bars_required:
            return False
            
        # Check if we have sufficient recent data
        recent_bars_threshold = min(10, len(df) // 4)
        recent_data_quality = df.tail(recent_bars_threshold).isna().sum().sum()
        
        return recent_data_quality == 0

    def _calculate_momentum_confirmation(self, df: pd.DataFrame, latest: pd.Series) -> float:
        """Calculate momentum confirmation score using price breakouts and volume surges"""
        if not self.config.get('momentum_confirmation_enabled', True):
            return 0.0
        
        confirmation_score = 0.0
        lookback = self.config.get('price_breakout_lookback', 20)
        volume_threshold = self.config.get('volume_surge_threshold', 2.5)
        
        if len(df) < lookback + 5:
            return 0.0
        
        try:
            # 1. Price Breakout Detection
            current_close = latest['close']
            
            # Resistance breakout (for long momentum)
            resistance_level = df['high'].tail(lookback).max()
            resistance_breakout = current_close > resistance_level
            
            # Support breakdown (for short momentum)  
            support_level = df['low'].tail(lookback).min()
            support_breakdown = current_close < support_level
            
            # 2. Volume Surge Detection
            current_volume = latest.get('volume_ratio', 1.0)
            volume_surge = current_volume > volume_threshold
            
            # 3. Recent momentum strength
            recent_highs = (df['close'] > df['close'].shift(1)).tail(5).sum()
            recent_momentum_strength = recent_highs / 5.0
            
            # Calculate confirmation score
            if resistance_breakout and volume_surge:
                confirmation_score += 0.6
            elif resistance_breakout:
                confirmation_score += 0.4
            elif support_breakdown and volume_surge:
                confirmation_score += 0.3  # Less weight for breakdowns
            
            if volume_surge:
                confirmation_score += 0.2
                
            confirmation_score += recent_momentum_strength * 0.2
            
            return min(confirmation_score, 1.0)
            
        except Exception:
            return 0.0

    def _enhance_momentum_score_with_confirmation(self, momentum_score: float, 
                                               confirmation_score: float) -> float:
        """Enhance momentum score with confirmation signals"""
        if confirmation_score > 0.5:  # Strong confirmation
            boost = confirmation_score * self.config.get('breakout_confirmation_weight', 0.3)
            return min(momentum_score * (1 + boost), 1.0)
        return momentum_score

    def calculate_enhanced_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Unified indicator calculation with error handling"""
        # Validate timeframe alignment first
        if not self._validate_timeframe_alignment(df):
            logger.debug(f"Insufficient data or timeframe alignment issues for indicators")
            return df
            
        if config.VECTORIZED_CALCULATIONS:
            return self.calculate_enhanced_indicators_vectorized(df)
        else:
            return self._calculate_enhanced_indicators_sequential(df)

    def calculate_enhanced_indicators_vectorized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Vectorized indicator calculation - 3-5x faster"""
        try:
            df_indicators = df.copy()
            min_bars_required = max(self.config['trend_ema_slow'], 
                                  self.config['bollinger_period'], 
                                  self.config['atr_period'], 20)
            
            if len(df_indicators) < min_bars_required:
                return df_indicators
            
            # Convert to numpy arrays for vectorized operations
            close = df_indicators['close'].values.astype(np.float64)
            high = df_indicators['high'].values.astype(np.float64)
            low = df_indicators['low'].values.astype(np.float64)
            volume = df_indicators['volume'].values.astype(np.float64)
            
            # Vectorized indicator calculations
            indicators = self._calculate_all_indicators_vectorized(close, high, low, volume)
            
            # Assign back to DataFrame
            for col, values in indicators.items():
                df_indicators[col] = values
            
            return df_indicators
                
        except Exception as e:
            logger.error(f"Error in vectorized indicators: {e}")
            return df

    def _calculate_all_indicators_vectorized(self, close: np.ndarray, high: np.ndarray, 
                                        low: np.ndarray, volume: np.ndarray) -> Dict[str, np.ndarray]:
        """Calculate all indicators in one pass using vectorized operations with safe division"""
        results = {}
        
        # Trend indicators
        results['ema_fast'] = talib.EMA(close, timeperiod=self.config['trend_ema_fast'])
        results['ema_slow'] = talib.EMA(close, timeperiod=self.config['trend_ema_slow'])
        results['adx'] = talib.ADX(high, low, close, timeperiod=self.config['trend_adx_period'])
        
        # Mean reversion indicators
        results['rsi'] = talib.RSI(close, timeperiod=self.config['mean_reversion_rsi_period'])
        bb_upper, bb_middle, bb_lower = talib.BBANDS(
            close, timeperiod=self.config['bollinger_period'],
            nbdevup=self.config['bollinger_std'], nbdevdn=self.config['bollinger_std']
        )
        results['bb_upper'] = bb_upper
        results['bb_middle'] = bb_middle
        results['bb_lower'] = bb_lower
        
        # Volatility indicators
        results['atr'] = talib.ATR(high, low, close, timeperiod=self.config['atr_period'])
        
        # Momentum indicators
        results['roc'] = talib.ROC(close, timeperiod=self.config['momentum_period'])
        macd, macd_signal, macd_hist = talib.MACD(close)
        results['macd'] = macd
        results['macd_signal'] = macd_signal
        results['macd_hist'] = macd_hist
        
        slowk, slowd = talib.STOCH(high, low, close)
        results['stoch_k'] = slowk
        results['stoch_d'] = slowd
        
        # Volume indicators
        volume_clean = np.nan_to_num(volume, nan=0.0, posinf=0.0, neginf=0.0)
        results['volume_ma'] = talib.SMA(volume_clean, timeperiod=self.config['volume_ma_period'])
        
        volume_ma_clean = np.nan_to_num(results['volume_ma'], nan=1.0)
        
        # Safe division for volume ratio
        with np.errstate(divide='ignore', invalid='ignore'):
            results['volume_ratio'] = np.where(
                volume_ma_clean > 0, 
                volume_clean / volume_ma_clean, 
                1.0
            )
            results['volume_ratio'] = np.nan_to_num(results['volume_ratio'], nan=1.0, posinf=1.0, neginf=1.0)
        
        results['obv'] = talib.OBV(close, volume_clean)
        
        # Derived metrics (vectorized) with safe division
        ema_fast_clean = np.nan_to_num(results['ema_fast'], nan=0)
        ema_slow_clean = np.nan_to_num(results['ema_slow'], nan=0)
        results['trend_direction'] = np.where(ema_fast_clean > ema_slow_clean, 1, -1)
        
        bb_upper_clean = np.nan_to_num(results['bb_upper'], nan=close)
        bb_lower_clean = np.nan_to_num(results['bb_lower'], nan=close)
        bb_range = bb_upper_clean - bb_lower_clean
        
        with np.errstate(divide='ignore', invalid='ignore'):
            bb_position = np.where(
                bb_range > 0, 
                (close - bb_lower_clean) / bb_range, 
                0.5
            )
            bb_position = np.nan_to_num(bb_position, nan=0.5, posinf=0.5, neginf=0.5)
            bb_position = np.clip(bb_position, 0.0, 1.0)
        
        results['bb_position'] = bb_position
        
        macd_clean = np.nan_to_num(results['macd'], nan=0)
        macd_signal_clean = np.nan_to_num(results['macd_signal'], nan=0)
        results['momentum_direction'] = np.where(macd_clean > macd_signal_clean, 1, -1)
        
        return results

    def _calculate_enhanced_indicators_sequential(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sequential indicator calculation (fallback)"""
        try:
            df_indicators = df.copy()
            min_bars_required = max(self.config['trend_ema_slow'], self.config['bollinger_period'], 
                                self.config['atr_period'], 20)
            
            if len(df_indicators) < min_bars_required:
                return df_indicators
            
            required_cols = ['open', 'high', 'low', 'close', 'volume']
            if not all(col in df_indicators.columns for col in required_cols):
                return df_indicators
            
            try:
                df_indicators[['open', 'high', 'low', 'close']] = df_indicators[['open', 'high', 'low', 'close']].apply(pd.to_numeric, errors='coerce')
                df_indicators['volume'] = df_indicators['volume'].apply(pd.to_numeric, errors='coerce').fillna(0)
            except Exception:
                return df_indicators
            
            df_indicators[['open', 'high', 'low', 'close']] = df_indicators[['open', 'high', 'low', 'close']].ffill().bfill()
            df_indicators['volume'] = df_indicators['volume'].fillna(0)
            
            close = df_indicators['close'].values.astype(np.float64)
            high = df_indicators['high'].values.astype(np.float64)
            low = df_indicators['low'].values.astype(np.float64)
            volume = df_indicators['volume'].values.astype(np.float64)
            
            self._calculate_trend_indicators(df_indicators, close, high, low)
            self._calculate_mean_reversion_indicators(df_indicators, close)
            self._calculate_volatility_indicators(df_indicators, high, low, close)
            self._calculate_momentum_indicators(df_indicators, close, high, low)
            self._calculate_volume_indicators(df_indicators, close, volume)
            self._calculate_derived_metrics(df_indicators, close)
            
            return df_indicators
                
        except Exception as e:
            return df

    def _calculate_trend_indicators(self, df: pd.DataFrame, close: np.ndarray, high: np.ndarray, low: np.ndarray):
        """Calculate unified trend indicators"""
        try:
            df['ema_fast'] = talib.EMA(close, timeperiod=self.config['trend_ema_fast'])
            df['ema_slow'] = talib.EMA(close, timeperiod=self.config['trend_ema_slow'])
            df['adx'] = talib.ADX(high, low, close, timeperiod=self.config['trend_adx_period'])
        except Exception:
            df['ema_fast'] = df['ema_slow'] = df['adx'] = np.nan

    def _calculate_mean_reversion_indicators(self, df: pd.DataFrame, close: np.ndarray):
        """Calculate unified mean reversion indicators"""
        try:
            df['rsi'] = talib.RSI(close, timeperiod=self.config['mean_reversion_rsi_period'])
            bb_upper, bb_middle, bb_lower = talib.BBANDS(
                close, timeperiod=self.config['bollinger_period'],
                nbdevup=self.config['bollinger_std'], nbdevdn=self.config['bollinger_std']
            )
            df['bb_upper'] = bb_upper
            df['bb_middle'] = bb_middle
            df['bb_lower'] = bb_lower
        except Exception:
            df['rsi'] = df['bb_upper'] = df['bb_middle'] = df['bb_lower'] = np.nan

    def _calculate_volatility_indicators(self, df: pd.DataFrame, high: np.ndarray, low: np.ndarray, close: np.ndarray):
        """Calculate unified volatility indicators"""
        try:
            df['atr'] = talib.ATR(high, low, close, timeperiod=self.config['atr_period'])
        except Exception:
            df['atr'] = np.nan

    def _calculate_momentum_indicators(self, df: pd.DataFrame, close: np.ndarray, high: np.ndarray, low: np.ndarray):
        """Calculate unified momentum indicators"""
        try:
            df['roc'] = talib.ROC(close, timeperiod=self.config['momentum_period'])
            macd, macd_signal, macd_hist = talib.MACD(close)
            df['macd'] = macd
            df['macd_signal'] = macd_signal
            df['macd_hist'] = macd_hist
            
            slowk, slowd = talib.STOCH(high, low, close)
            df['stoch_k'] = slowk
            df['stoch_d'] = slowd
        except Exception:
            df['roc'] = df['macd'] = df['macd_signal'] = df['macd_hist'] = np.nan
            df['stoch_k'] = df['stoch_d'] = np.nan

    def _calculate_volume_indicators(self, df: pd.DataFrame, close: np.ndarray, volume: np.ndarray):
        """Calculate unified volume indicators with safe division"""
        try:
            volume_clean = np.nan_to_num(volume, nan=0.0, posinf=0.0, neginf=0.0)
            df['volume_ma'] = talib.SMA(volume_clean, timeperiod=self.config['volume_ma_period'])
            
            volume_ma_clean = df['volume_ma'].fillna(1.0)
            
            with np.errstate(divide='ignore', invalid='ignore'):
                volume_ratio = np.where(
                    volume_ma_clean > 0, 
                    volume_clean / volume_ma_clean, 
                    1.0
                )
                volume_ratio = np.nan_to_num(volume_ratio, nan=1.0, posinf=1.0, neginf=1.0)
            
            df['volume_ratio'] = volume_ratio
            df['obv'] = talib.OBV(close, volume_clean)
        except Exception:
            df['volume_ma'] = np.nan
            df['volume_ratio'] = 1.0
            df['obv'] = np.nan

    def _calculate_derived_metrics(self, df: pd.DataFrame, close: np.ndarray):
        """Calculate unified derived metrics with safe division"""
        try:
            ema_fast_clean = df['ema_fast'].fillna(0)
            ema_slow_clean = df['ema_slow'].fillna(0)
            df['trend_direction'] = np.where(ema_fast_clean > ema_slow_clean, 1, -1)
            
            bb_upper_clean = df['bb_upper'].fillna(close)
            bb_lower_clean = df['bb_lower'].fillna(close)
            bb_range = bb_upper_clean - bb_lower_clean
            
            with np.errstate(divide='ignore', invalid='ignore'):
                bb_position = np.where(
                    bb_range > 0, 
                    (close - bb_lower_clean) / bb_range, 
                    0.5
                )
                bb_position = np.nan_to_num(bb_position, nan=0.5, posinf=0.5, neginf=0.5)
                bb_position = np.clip(bb_position, 0.0, 1.0)
            
            df['bb_position'] = bb_position
            
            macd_clean = df['macd'].fillna(0)
            macd_signal_clean = df['macd_signal'].fillna(0)
            df['momentum_direction'] = np.where(macd_clean > macd_signal_clean, 1, -1)
        except Exception:
            df['trend_direction'] = 0
            df['bb_position'] = 0.5
            df['momentum_direction'] = 0

    def calculate_enhanced_scores(self, df: pd.DataFrame) -> Dict[str, float]:
        """Unified score calculation"""
        if config.VECTORIZED_CALCULATIONS:
            return self.calculate_enhanced_scores_vectorized(df)
        else:
            return self._calculate_enhanced_scores_sequential(df)

    def calculate_enhanced_scores_vectorized(self, df: pd.DataFrame) -> Dict[str, float]:
        """Vectorized score calculation with enhanced momentum"""
        if len(df) < 2:
            return self._get_default_score_data()
            
        latest = df.iloc[-1]
        
        trend_score = self._calculate_trend_score_vectorized(latest)
        mean_reversion_score = self._calculate_mean_reversion_score_vectorized(latest)
        
        # ENHANCED: Pass dataframe for momentum confirmation
        momentum_score = self._calculate_momentum_score_vectorized(latest, df)
        
        volume_score = self._calculate_volume_score_vectorized(latest)
        volatility_score = self._calculate_volatility_score_vectorized(latest)
        
        total_score = (trend_score + mean_reversion_score + volume_score + 
                      momentum_score + volatility_score)
        confidence = self._calculate_confidence_vectorized(total_score, latest)
        
        return {
            'trend_score': trend_score,
            'mean_reversion_score': mean_reversion_score,
            'volume_score': volume_score,
            'momentum_score': momentum_score,
            'volatility_score': volatility_score,
            'total_score': total_score,
            'confidence': confidence,
            'trend_strength': min(latest.get('adx', 0) / 60, 1.0),
            'mean_reversion_strength': self._calculate_mean_reversion_strength_vectorized(latest),
            'momentum_strength': self._calculate_momentum_strength_vectorized(latest),
            'trend_direction': latest.get('trend_direction', 0),
            'momentum_direction': latest.get('momentum_direction', 0),
            'aligned_indicators': self._count_aligned_indicators_vectorized(latest)
        }

    def _calculate_enhanced_scores_sequential(self, df: pd.DataFrame) -> Dict[str, float]:
        """Sequential score calculation (fallback)"""
        if len(df) < 2:
            return self._get_default_score_data()
            
        latest = df.iloc[-1]
        
        required_indicators = ['ema_fast', 'ema_slow', 'rsi', 'adx', 'atr', 'volume_ratio', 'roc', 'macd']
        if not all(pd.notna(latest.get(col, np.nan)) for col in required_indicators):
            return self._get_default_score_data()
        
        try:
            trend_score = self._calculate_trend_score(latest)
            mean_reversion_score = self._calculate_mean_reversion_score(latest)
            
            # ENHANCED: Pass dataframe for momentum confirmation
            momentum_score = self._calculate_momentum_score(latest, df)
            
            volume_score = self._calculate_volume_score(latest)
            volatility_score = self._calculate_volatility_score(latest)
            
            total_score = (trend_score + mean_reversion_score + volume_score + momentum_score + volatility_score)
            confidence = self._calculate_confidence(total_score, latest)
            
            return {
                'trend_score': trend_score,
                'mean_reversion_score': mean_reversion_score,
                'volume_score': volume_score,
                'momentum_score': momentum_score,
                'volatility_score': volatility_score,
                'total_score': total_score,
                'confidence': confidence,
                'trend_strength': min(latest['adx'] / 60, 1.0),
                'mean_reversion_strength': self._calculate_mean_reversion_strength(latest),
                'momentum_strength': self._calculate_momentum_strength(latest),
                'trend_direction': latest['trend_direction'],
                'momentum_direction': latest.get('momentum_direction', 0),
                'aligned_indicators': self._count_aligned_indicators(latest)
            }
            
        except Exception:
            return self._get_default_score_data()

    def _calculate_trend_score(self, latest: pd.Series) -> float:
        """Unified trend score calculation"""
        trend_strength = min(latest['adx'] / 60, 1.0)
        trend_consistency = 1.0
        return trend_strength * trend_consistency * self.config['weight_trend_strength']

    def _calculate_trend_score_vectorized(self, latest: pd.Series) -> float:
        """Vectorized trend score calculation"""
        trend_strength = min(latest.get('adx', 0) / 60, 1.0)
        return trend_strength * self.config['weight_trend_strength']

    def _calculate_mean_reversion_score(self, latest: pd.Series) -> float:
        """Unified mean reversion score calculation"""
        rsi_score = self._calculate_rsi_score(latest)
        bb_score = self._calculate_bb_score(latest)
        mean_reversion_strength = (rsi_score * 0.6 + bb_score * 0.4)
        return mean_reversion_strength * self.config['weight_mean_reversion']

    def _calculate_mean_reversion_score_vectorized(self, latest: pd.Series) -> float:
        """Vectorized mean reversion score calculation"""
        rsi_score = self._calculate_rsi_score_vectorized(latest)
        bb_score = self._calculate_bb_score_vectorized(latest)
        mean_reversion_strength = (rsi_score * 0.6 + bb_score * 0.4)
        return mean_reversion_strength * self.config['weight_mean_reversion']

    def _calculate_rsi_score(self, latest: pd.Series) -> float:
        """Calculate RSI-based score"""
        rsi = latest['rsi']
        trend_direction = latest['trend_direction']
        
        if trend_direction > 0:
            rsi_score = max(0, (self.config['rsi_oversold'] - rsi) / self.config['rsi_oversold'])
            if rsi < 25:
                rsi_score *= 0.7
        else:
            rsi_score = max(0, (rsi - self.config['rsi_overbought']) / (100 - self.config['rsi_overbought']))
            if rsi > 85:
                rsi_score *= 0.7
        return rsi_score

    def _calculate_rsi_score_vectorized(self, latest: pd.Series) -> float:
        """Vectorized RSI score calculation"""
        rsi = latest.get('rsi', 50)
        trend_direction = latest.get('trend_direction', 0)
        
        if trend_direction > 0:
            return max(0, (self.config['rsi_oversold'] - rsi) / self.config['rsi_oversold'])
        else:
            return max(0, (rsi - self.config['rsi_overbought']) / (100 - self.config['rsi_overbought']))

    def _calculate_bb_score(self, latest: pd.Series) -> float:
        """Calculate Bollinger Band score"""
        bb_position = latest.get('bb_position', 0.5)
        trend_direction = latest['trend_direction']
        
        if trend_direction > 0:
            return max(0, (0.2 - bb_position) / 0.2)
        else:
            return max(0, (bb_position - 0.8) / 0.2)

    def _calculate_bb_score_vectorized(self, latest: pd.Series) -> float:
        """Vectorized Bollinger Band score calculation"""
        bb_position = latest.get('bb_position', 0.5)
        trend_direction = latest.get('trend_direction', 0)
        
        if trend_direction > 0:
            return max(0, (0.2 - bb_position) / 0.2)
        else:
            return max(0, (bb_position - 0.8) / 0.2)

    def _calculate_momentum_score(self, latest: pd.Series, df: pd.DataFrame = None) -> float:
        """Enhanced momentum score calculation with confirmation"""
        roc_strength = min(abs(latest.get('roc', 0)) / 10, 1.0)
        macd_strength = self._calculate_macd_strength(latest)
        stoch_momentum = self._calculate_stoch_momentum(latest)
        
        momentum_strength = (roc_strength * 0.4 + macd_strength * 0.4 + abs(stoch_momentum) * 0.2)
        
        # Add momentum confirmation if dataframe is available
        if df is not None:
            confirmation_score = self._calculate_momentum_confirmation(df, latest)
            momentum_strength = self._enhance_momentum_score_with_confirmation(
                momentum_strength, confirmation_score
            )
        
        return momentum_strength * self.config['weight_momentum']

    def _calculate_momentum_score_vectorized(self, latest: pd.Series, df: pd.DataFrame = None) -> float:
        """Enhanced vectorized momentum score calculation with confirmation"""
        roc_strength = min(abs(latest.get('roc', 0)) / 10, 1.0)
        macd_strength = self._calculate_macd_strength_vectorized(latest)
        
        momentum_strength = (roc_strength * 0.4 + macd_strength * 0.4)
        
        # Add momentum confirmation if dataframe is available
        if df is not None:
            confirmation_score = self._calculate_momentum_confirmation(df, latest)
            momentum_strength = self._enhance_momentum_score_with_confirmation(
                momentum_strength, confirmation_score
            )
        
        return momentum_strength * self.config['weight_momentum']

    def _calculate_macd_strength(self, latest: pd.Series) -> float:
        """Calculate MACD strength"""
        if pd.notna(latest.get('macd_hist', np.nan)):
            macd_hist = latest['macd_hist']
            return min(abs(macd_hist) / (latest['close'] * 0.02), 1.0)
        return 0

    def _calculate_macd_strength_vectorized(self, latest: pd.Series) -> float:
        """Vectorized MACD strength calculation"""
        macd_hist = latest.get('macd_hist', 0)
        close_price = latest.get('close', 100)
        return min(abs(macd_hist) / (close_price * 0.02), 1.0)

    def _calculate_stoch_momentum(self, latest: pd.Series) -> float:
        """Calculate Stochastic momentum"""
        if pd.notna(latest.get('stoch_k', np.nan)) and pd.notna(latest.get('stoch_d', np.nan)):
            stoch_k, stoch_d = latest['stoch_k'], latest['stoch_d']
            if stoch_k > stoch_d and stoch_k > 50:
                return (stoch_k - 50) / 50
            elif stoch_k < stoch_d and stoch_k < 50:
                return (50 - stoch_k) / 50
        return 0

    def _calculate_volume_score(self, latest: pd.Series) -> float:
        """Unified volume score calculation"""
        volume_ratio = latest['volume_ratio'] if pd.notna(latest['volume_ratio']) else 1.0
        obv_strength = self._calculate_obv_strength(latest)
        
        return (min(volume_ratio / 3, 1.0) * 0.6 + obv_strength * 0.4) * self.config['weight_volume']

    def _calculate_volume_score_vectorized(self, latest: pd.Series) -> float:
        """Vectorized volume score calculation"""
        volume_ratio = latest.get('volume_ratio', 1.0)
        return (min(volume_ratio / 3, 1.0) * 0.6 + 0.4) * self.config['weight_volume']

    def _calculate_obv_strength(self, latest: pd.Series) -> float:
        """Calculate OBV strength"""
        if pd.notna(latest.get('obv', np.nan)):
            return 1.0 if latest['obv'] > 0 else 0.3
        return 0

    def _calculate_volatility_score(self, latest: pd.Series) -> float:
        """Unified volatility score calculation"""
        atr_pct = latest['atr'] / latest['close'] if latest['atr'] > 0 else 0.02
        
        if atr_pct < 0.015:
            return (atr_pct / 0.015) * self.config['weight_volatility']
        elif atr_pct > 0.04:
            return max(0, 1 - (atr_pct - 0.04) / 0.03) * self.config['weight_volatility']
        else:
            return 1.0 * self.config['weight_volatility']

    def _calculate_volatility_score_vectorized(self, latest: pd.Series) -> float:
        """Vectorized volatility score calculation"""
        atr = latest.get('atr', 0)
        close = latest.get('close', 100)
        atr_pct = atr / close if atr > 0 else 0.02
        
        if atr_pct < 0.015:
            return (atr_pct / 0.015) * self.config['weight_volatility']
        elif atr_pct > 0.04:
            return max(0, 1 - (atr_pct - 0.04) / 0.03) * self.config['weight_volatility']
        else:
            return 1.0 * self.config['weight_volatility']

    def _calculate_confidence(self, total_score: float, latest: pd.Series) -> float:
        """Unified confidence calculation"""
        base_confidence = min(total_score / (
            self.config['weight_trend_strength'] + 
            self.config['weight_mean_reversion'] + 
            self.config['weight_momentum']
        ), 1.0)
        
        confirmation_bonus = self._count_aligned_indicators(latest) * 0.1
        return min(base_confidence + confirmation_bonus, 1.0)

    def _calculate_confidence_vectorized(self, total_score: float, latest: pd.Series) -> float:
        """Vectorized confidence calculation"""
        base_confidence = min(total_score / (
            self.config['weight_trend_strength'] + 
            self.config['weight_mean_reversion'] + 
            self.config['weight_momentum']
        ), 1.0)
        
        confirmation_bonus = self._count_aligned_indicators_vectorized(latest) * 0.1
        return min(base_confidence + confirmation_bonus, 1.0)

    def _count_aligned_indicators(self, latest: pd.Series) -> int:
        """Count aligned indicators for confidence bonus"""
        aligned = 0
        if latest['trend_direction'] == latest.get('momentum_direction', 0):
            aligned += 1
        if latest['volume_ratio'] > self.config['min_volume_ratio']:
            aligned += 1
        if self._calculate_mean_reversion_strength(latest) > 0.3:
            aligned += 1
        return aligned

    def _count_aligned_indicators_vectorized(self, latest: pd.Series) -> int:
        """Vectorized aligned indicators count"""
        aligned = 0
        if latest.get('trend_direction', 0) == latest.get('momentum_direction', 0):
            aligned += 1
        if latest.get('volume_ratio', 1.0) > self.config['min_volume_ratio']:
            aligned += 1
        if self._calculate_mean_reversion_strength_vectorized(latest) > 0.3:
            aligned += 1
        return aligned

    def _calculate_mean_reversion_strength(self, latest: pd.Series) -> float:
        """Calculate mean reversion strength"""
        rsi_score = self._calculate_rsi_score(latest)
        bb_score = self._calculate_bb_score(latest)
        return (rsi_score * 0.6 + bb_score * 0.4)

    def _calculate_mean_reversion_strength_vectorized(self, latest: pd.Series) -> float:
        """Vectorized mean reversion strength calculation"""
        rsi_score = self._calculate_rsi_score_vectorized(latest)
        bb_score = self._calculate_bb_score_vectorized(latest)
        return (rsi_score * 0.6 + bb_score * 0.4)

    def _calculate_momentum_strength(self, latest: pd.Series) -> float:
        """Calculate momentum strength"""
        roc_strength = min(abs(latest.get('roc', 0)) / 10, 1.0)
        macd_strength = self._calculate_macd_strength(latest)
        stoch_momentum = abs(self._calculate_stoch_momentum(latest))
        return (roc_strength * 0.4 + macd_strength * 0.4 + stoch_momentum * 0.2)

    def _calculate_momentum_strength_vectorized(self, latest: pd.Series) -> float:
        """Vectorized momentum strength calculation"""
        roc_strength = min(abs(latest.get('roc', 0)) / 10, 1.0)
        macd_strength = self._calculate_macd_strength_vectorized(latest)
        return (roc_strength * 0.4 + macd_strength * 0.4)

    def _get_default_score_data(self) -> Dict[str, float]:
        """Return default score data"""
        return {
            'trend_score': 0, 'mean_reversion_score': 0, 'volume_score': 0, 'momentum_score': 0,
            'volatility_score': 0, 'total_score': 0, 'confidence': 0, 'trend_strength': 0,
            'mean_reversion_strength': 0, 'momentum_strength': 0, 'trend_direction': 0,
            'momentum_direction': 0, 'aligned_indicators': 0
        }

    def determine_enhanced_signal_type(self, latest: pd.Series, score_data: Dict[str, float], 
                                 df: pd.DataFrame = None) -> str:
        """Enhanced signal determination with momentum confirmation"""
        try:
            trend_direction = score_data.get('trend_direction', 0)
            momentum_direction = score_data.get('momentum_direction', 0)
            rsi = latest.get('rsi', 50)
            bb_position = latest.get('bb_position', 0.5)
            total_score = score_data.get('total_score', 0)
            confidence = score_data.get('confidence', 0)
            volume_ratio = latest.get('volume_ratio', 1.0)
            adx = latest.get('adx', 0)
            
            long_score = 0.0
            short_score = 0.0
            neutral_score = 0.0
            
            # Neutral criteria
            if (trend_direction > 0 and momentum_direction < 0) or (trend_direction < 0 and momentum_direction > 0):
                neutral_score += 1.5
            
            if adx < 15:
                neutral_score += 1.0
            elif adx < 20:
                neutral_score += 0.5
            
            if 40 <= rsi <= 60:
                neutral_score += 1.0
            
            if 0.4 <= bb_position <= 0.6:
                neutral_score += 1.0
            
            if 0.9 <= volume_ratio <= 1.3:
                neutral_score += 0.75
            
            if 0.3 <= total_score <= 0.5:
                neutral_score += 0.75
            
            if 0.4 <= confidence <= 0.6:
                neutral_score += 0.75
            
            # Long criteria
            if trend_direction > 0 and momentum_direction > 0:
                long_score += 4.0
            elif trend_direction > 0 or momentum_direction > 0:
                long_score += 2.0
            
            if rsi < 25:
                long_score += 3.0
            elif rsi < 32:
                long_score += 2.5
            elif rsi < 38:
                long_score += 1.5
            
            if bb_position < 0.15:
                long_score += 2.5
            elif bb_position < 0.25:
                long_score += 1.5
            elif bb_position < 0.35:
                long_score += 0.5
            
            if volume_ratio > 1.8:
                long_score += 2.0
            elif volume_ratio > 1.4:
                long_score += 1.5
            elif volume_ratio > 1.2:
                long_score += 0.5
            
            if adx > 35:
                long_score += 2.5
            elif adx > 28:
                long_score += 1.5
            elif adx > 22:
                long_score += 0.5
            
            # Short criteria
            if trend_direction < 0 and momentum_direction < 0:
                short_score += 4.0
            elif trend_direction < 0 or momentum_direction < 0:
                short_score += 2.0
            
            if rsi > 75:
                short_score += 3.0
            elif rsi > 68:
                short_score += 2.5
            elif rsi > 62:
                short_score += 1.5
            
            if bb_position > 0.85:
                short_score += 2.5
            elif bb_position > 0.75:
                short_score += 1.5
            elif bb_position > 0.65:
                short_score += 0.5
            
            if volume_ratio > 1.8:
                short_score += 2.0
            elif volume_ratio > 1.4:
                short_score += 1.5
            elif volume_ratio > 1.2:
                short_score += 0.5
            
            if adx > 35:
                short_score += 2.5
            elif adx > 28:
                short_score += 1.5
            elif adx > 22:
                short_score += 0.5
            
            # ENHANCEMENT: Add bonus for strong momentum confirmation
            if df is not None:
                momentum_confirmation = self._calculate_momentum_confirmation(df, latest)
                if momentum_confirmation > 0.7:
                    if trend_direction > 0 and momentum_direction > 0:
                        long_score += 1.5
                    elif trend_direction < 0 and momentum_direction < 0:
                        short_score += 1.5
            
            # Apply confidence multiplier
            confidence_multiplier = confidence
            long_score *= confidence_multiplier
            short_score *= confidence_multiplier
            neutral_score *= confidence_multiplier
            
            if confidence > 0.7:
                long_score *= 1.2
                short_score *= 1.2
            
            # Final decision
            signal_scores = {
                "LONG": long_score,
                "SHORT": short_score, 
                "NEUTRAL": neutral_score
            }
            
            winning_signal = max(signal_scores.items(), key=lambda x: x[1])
            signal_type, winning_score = winning_signal
            
            min_winning_score = 1.0
            
            if winning_score < min_winning_score:
                return "NEUTRAL"
            
            if signal_type == "LONG":
                bullish_conditions = [
                    trend_direction > 0,
                    momentum_direction > 0,
                    rsi < 45,
                    bb_position < 0.6,
                    latest.get('macd_hist', 0) > 0
                ]
                if sum(bullish_conditions) < 2:
                    return "NEUTRAL"
            elif signal_type == "SHORT":
                bearish_conditions = [
                    trend_direction < 0,
                    momentum_direction < 0,
                    rsi > 55,
                    bb_position > 0.4,
                    latest.get('macd_hist', 0) < 0
                ]
                if sum(bearish_conditions) < 2:
                    return "NEUTRAL"
            
            return signal_type
            
        except Exception:
            return "NEUTRAL"

    def calculate_ticker_ranking(self, ticker: str, df: pd.DataFrame) -> Optional[TickerRanking]:
        """Calculate enhanced ranking metrics for a ticker"""
        return self.calculate_ticker_ranking_optimized(ticker, df)

    def calculate_ticker_ranking_optimized(self, ticker: str, df: pd.DataFrame) -> Optional[TickerRanking]:
        """Optimized ranking calculation using vectorized operations with enhanced momentum"""
        try:
            min_bars_required = max(
                self.config['trend_ema_slow'], 
                self.config['bollinger_period'], 
                self.config['atr_period'],
                self.config.get('price_breakout_lookback', 20),  # NEW: Include breakout lookback
                20
            )
            
            if len(df) < min_bars_required:
                return None
                
            df_with_indicators = self.calculate_enhanced_indicators(df)
            
            if df_with_indicators.empty:
                return None
                
            latest = df_with_indicators.iloc[-1]
            
            required_indicators = ['ema_fast', 'ema_slow', 'rsi', 'adx', 'atr', 'volume_ratio']
            if any(pd.isna(latest.get(col, np.nan)) for col in required_indicators):
                return None
            
            # ENHANCED: Pass dataframe for momentum confirmation
            score_data = self.calculate_enhanced_scores(df_with_indicators)
            signal_type = self.determine_enhanced_signal_type(latest, score_data, df_with_indicators)
            
            ticker_info = {
                'name': ticker,
                'primary_exchange': 'Unknown'
            }
            
            volume_confirmation = min(latest['volume_ratio'] / 2, 1.0) if pd.notna(latest['volume_ratio']) else 0.5
            
            return TickerRanking(
                ticker=ticker,
                name=ticker_info['name'],
                primary_exchange=ticker_info['primary_exchange'],
                total_score=score_data['total_score'],
                trend_score=score_data['trend_score'],
                mean_reversion_score=score_data['mean_reversion_score'],
                volume_score=score_data['volume_score'],
                volatility_score=score_data['volatility_score'],
                momentum_score=score_data['momentum_score'],
                confidence=score_data['confidence'],
                trend_strength=score_data['trend_strength'],
                mean_reversion_strength=score_data['mean_reversion_strength'],
                volume_confirmation=volume_confirmation,
                signal_type=signal_type,
                current_price=latest['close'],
                timestamp=df.index[-1].strftime('%Y-%m-%d %H:%M:%S') if hasattr(df.index[-1], 'strftime') else str(df.index[-1])
            )
            
        except Exception:
            return None

# ======================== COMBINED RANKING ENGINE ======================== #
class CombinedRankingEngine:
    """Unified ranking engine that handles pure ranking without sectors"""
    
    def __init__(self, ticker_scanner, strategy_config: Dict[str, Any] = None):
        self.ticker_scanner = ticker_scanner
        self.strategy = MomentumMeanReversionStrategy(strategy_config)
        self.polygon_provider = DataProvider(config.POLYGON_API_KEY)
        
        # Performance optimization
        self.num_cores = mp.cpu_count()
        self.process_pool_size = min(self.num_cores, config.PARALLEL_WORKERS)
        
        # Run status manager for tracking completion
        self.run_status_manager = RunStatusManager()
        
        logger.info(f"Pure Ranking Engine initialized with {self.process_pool_size} parallel workers")
        
    @monitor_performance
    @handle_errors()
    async def rank_all_tickers(self, max_tickers: int = None) -> List[TickerRanking]:
        """Main ranking method - uses optimized version by default"""
        return await self.rank_all_tickers_optimized(max_tickers)

    async def rank_all_tickers_optimized(self, max_tickers: int = None) -> List[TickerRanking]:
        """Optimized pure ranking without sectors"""
        logger.info("🚀 Starting pure ranking analysis")
        
        # Get available capital for trading context
        available_capital = await self.ticker_scanner.get_trading_capital()
        logger.info(f"Available trading capital: ${available_capital:.2f}")
        
        # Get existing positions to avoid duplicates
        existing_positions = await self.ticker_scanner.alpaca_manager.get_positions()
        existing_tickers = {pos['symbol'] for pos in existing_positions}
        logger.info(f"Found {len(existing_tickers)} existing positions")
        
        # Get tickers
        all_tickers = await self.ticker_scanner.db.get_all_active_tickers()
        if not all_tickers:
            return []
            
        # Apply limit
        max_tickers_to_use = max_tickers if max_tickers is not None else config.MAX_TICKERS_TO_RANK
        if max_tickers_to_use is not None and len(all_tickers) > max_tickers_to_use:
            tickers_to_rank = all_tickers[:max_tickers_to_use]
        else:
            tickers_to_rank = all_tickers
            
        rankings = []
        
        # Use tqdm with simpler description
        with tqdm(total=len(tickers_to_rank), desc="Ranking", unit="ticker") as progress_bar:
            batch_size = min(config.OPTIMIZED_BATCH_SIZE, max(10, len(tickers_to_rank) // self.process_pool_size))
            
            for i in range(0, len(tickers_to_rank), batch_size):
                if self.ticker_scanner.shutdown_requested:
                    break
                    
                batch = tickers_to_rank[i:i + batch_size]
                batch_rankings = await self._process_ticker_batch_parallel(batch)
                successful_rankings = [r for r in batch_rankings if r is not None]
                rankings.extend(successful_rankings)
                progress_bar.update(len(batch))
                progress_bar.set_postfix({
                    'success': len(successful_rankings),
                    'total': len(rankings)
                })
                
                if i + batch_size < len(tickers_to_rank):
                    await asyncio.sleep(0.5)
        
        # Sort and rank - PURE RANKING without sectors
        valid_rankings = [r for r in rankings if r is not None]
        valid_rankings.sort(key=lambda x: x.total_score, reverse=True)
        
        for i, ranking in enumerate(valid_rankings):
            ranking.rank = i + 1
        
        # ========== ADDED LOGGING SECTION ==========
        # Performance summary
        signal_counts = {}
        for ranking in valid_rankings:
            signal_type = ranking.signal_type
            signal_counts[signal_type] = signal_counts.get(signal_type, 0) + 1
        
        # SINGLE summary log
        logger.info(f"Pure ranking complete: {len(valid_rankings)} tickers, signals: {signal_counts}")
        # ========== END OF ADDED CODE ==========
        
        return valid_rankings

    async def _process_ticker_batch_parallel(self, ticker_batch: List[Dict]) -> List[TickerRanking]:
        """Process batch using true parallel processing with ProcessPoolExecutor"""
        rankings = []
        
        # Fetch data for all tickers in batch first
        ticker_data_map = {}
        data_fetch_tasks = []
        
        for ticker_data in ticker_batch:
            ticker = ticker_data['ticker']
            task = asyncio.create_task(self._fetch_price_data_for_ranking(ticker))
            data_fetch_tasks.append((ticker, task))
            ticker_data_map[ticker] = ticker_data
        
        # Wait for all data to be fetched
        for ticker, task in data_fetch_tasks:
            try:
                df = await asyncio.wait_for(task, timeout=30.0)
                if df is not None and not df.empty and len(df) >= 30:
                    ticker_data_map[ticker]['df'] = df
                else:
                    del ticker_data_map[ticker]
            except (asyncio.TimeoutError, Exception):
                if ticker in ticker_data_map:
                    del ticker_data_map[ticker]
        
        if not ticker_data_map:
            return []
        
        # Use ProcessPoolExecutor for CPU-bound ranking calculations
        with ProcessPoolExecutor(max_workers=self.process_pool_size) as executor:
            tasks = {}
            for ticker, data in ticker_data_map.items():
                if 'df' in data:
                    future = executor.submit(
                        self._rank_single_ticker_worker,
                        ticker,
                        data,
                        self.strategy.config
                    )
                    tasks[future] = ticker
            
            # Collect results as they complete
            for future in as_completed(tasks):
                try:
                    result = future.result(timeout=30.0)
                    if result is not None:
                        rankings.append(result)
                except Exception:
                    pass
        
        return rankings
    
    @staticmethod
    def _rank_single_ticker_worker(ticker: str, ticker_data: Dict, strategy_config: Dict) -> Optional[TickerRanking]:
        """Worker function for parallel processing - must be static for pickling"""
        try:
            strategy = MomentumMeanReversionStrategy(strategy_config)
            df = ticker_data.get('df')
            
            if df is None or df.empty or len(df) < 30:
                return None
            
            ranking = strategy.calculate_ticker_ranking_optimized(ticker, df)
            if ranking:
                ranking.name = ticker_data.get('name', ticker)
                ranking.primary_exchange = ticker_data.get('primary_exchange', 'Unknown')
            
            return ranking
        except Exception:
            return None

    async def _fetch_price_data_for_ranking(self, ticker: str) -> Optional[pd.DataFrame]:
        """Fetch price data specifically for ranking with improved error handling"""
        try:
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
            
            df = await self.polygon_provider.get_historical_bars_optimized(ticker, "1day", start_date, end_date)
            
            if df.empty:
                return None
                
            if len(df) < 20:
                return None
                
            required_cols = ['open', 'high', 'low', 'close', 'volume']
            if not all(col in df.columns for col in required_cols):
                return None
                
            try:
                df[['open', 'high', 'low', 'close']] = df[['open', 'high', 'low', 'close']].apply(pd.to_numeric, errors='coerce')
                df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
                df = df.dropna(subset=['open', 'high', 'low', 'close'])
                
                if df.empty:
                    return None
                    
            except Exception:
                return None
                
            return df
            
        except Exception:
            return None

    @monitor_performance
    @handle_errors()
    async def save_rankings_to_csv(self, rankings: List[TickerRanking], filename: str = None) -> str:
        """Save pure rankings to CSV file without sector organization"""
        if not rankings:
            return ""
        
        # Create directory if it doesn't exist
        os.makedirs(config.RANKING_RESULTS_DIR, exist_ok=True)
        
        # Generate filename if not provided
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"pure_ticker_rankings_{timestamp}.csv"
        
        filepath = os.path.join(config.RANKING_RESULTS_DIR, filename)
        
        # Convert to DataFrame
        df_data = []
        for ranking in rankings:
            df_data.append({
                'Rank': ranking.rank,
                'Ticker': ranking.ticker,
                'Name': ranking.name,
                'Exchange': ranking.primary_exchange,
                'Total_Score': ranking.total_score,
                'Trend_Score': ranking.trend_score,
                'MeanReversion_Score': ranking.mean_reversion_score,
                'Volume_Score': ranking.volume_score,
                'Volatility_Score': ranking.volatility_score,
                'Momentum_Score': ranking.momentum_score,
                'Confidence': ranking.confidence,
                'Trend_Strength': ranking.trend_strength,
                'MeanReversion_Strength': ranking.mean_reversion_strength,
                'Volume_Confirmation': ranking.volume_confirmation,
                'Signal_Type': ranking.signal_type,
                'Current_Price': ranking.current_price,
                'Timestamp': ranking.timestamp or datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        
        df = pd.DataFrame(df_data)
        df.to_csv(filepath, index=False)
        
        logger.info(f"Pure rankings saved to {filepath}")
        return filepath

# ======================== TICKER SCANNER ======================== #
class PolygonTickerScanner:
    """Unified ticker scanner with efficient data retrieval"""
    
    def __init__(self):
        self.api_key = config.POLYGON_API_KEY
        self.base_url = "https://api.polygon.io/v3/reference/tickers"
        self.composite_indices = config.COMPOSITE_INDICES
        self.active = False
        self.shutdown_requested = False
        self.semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_REQUESTS)
        
        # Initialize unified components
        self.db = DatabaseManager()
        self.market_calendar = mcal.get_calendar(config.MARKET_CALENDAR)
        self.local_tz = get_localzone()
        
        # Add Alpaca trading manager
        self.alpaca_manager = AlpacaTradingManager()
        
        # NEW: Add Capital Position Detector
        self.capital_detector = CapitalPositionDetector(self.alpaca_manager)
        
        # NEW: Add Trade Executor
        self.trade_executor = None
        
        # Performance
        self.performance_metrics = {}
        self.api_error_count = 0
        
        # Cache and locks
        self.cache_lock = RLock()
        self.refresh_lock = Lock()
        self.known_missing_tickers = set()
        self.initial_refresh_complete = Event()
        self.last_refresh_time = 0
        self.ticker_cache = pd.DataFrame(columns=[
            "ticker", "name", "primary_exchange", "last_updated_utc", "type", "market", "locale"
        ])
        self.current_tickers_set = set()
        
        # Strategy engines
        self.combined_ranking_engine = None
        
        # Display manager
        self.display_manager = display_manager
        
        logger.info(f"Pure Ticker Scanner initialized with {len(self.composite_indices)} composite indices")

    async def start(self):
        """Unified startup method - NOW WITH CAPITAL DETECTION FIRST"""
        if not self.active:
            self.active = True
            self.shutdown_requested = False
            await self.db.initialize()  # Ensure database is initialized
            
            # STEP 1: Initialize trading manager
            trading_initialized = await self.alpaca_manager.initialize()
            if trading_initialized:
                logger.info("✅ Alpaca trading manager initialized successfully")
            else:
                logger.warning("Alpaca trading not available - proceeding without trading capabilities")
            
            # STEP 2: DETECT CAPITAL AND POSITIONS FIRST
            capital_detected = await self.capital_detector.detect_capital_and_positions()
            if capital_detected:
                logger.info("✅ Capital and position detection completed successfully")
            else:
                logger.warning("❌ Capital and position detection failed")
                
            # STEP 3: Initialize trade executor
            await self.initialize_trade_executor()
                
            # STEP 4: Continue with existing pipeline
            await self._init_cache()
            self.initial_refresh_complete.set()
            logger.info("Pure Ticker Scanner started")

    async def shutdown(self):
        """Unified shutdown method"""
        self.active = False
        self.shutdown_requested = True
        
        # Stop trade executor
        if self.trade_executor:
            await self.trade_executor.stop()
        
        # Close database connections
        await self.db.close_all_connections()
            
        logger.info("Pure Ticker Scanner shutdown complete")

    async def _init_cache(self):
        """Unified cache initialization"""
        try:
            # Load active tickers from database
            db_tickers = await self.db.get_all_active_tickers()
            
            if db_tickers:
                self.ticker_cache = pd.DataFrame(db_tickers)
                
                # Find the ticker column
                column_names_lower = [str(col).lower() for col in self.ticker_cache.columns]
                if 'ticker' in column_names_lower:
                    ticker_col_idx = column_names_lower.index('ticker')
                    ticker_col = self.ticker_cache.columns[ticker_col_idx]
                    self.current_tickers_set = set(self.ticker_cache[ticker_col].tolist())
                else:
                    for col in self.ticker_cache.columns:
                        if any(keyword in str(col).lower() for keyword in ['symbol', 'ticker', 'code', 'id']):
                            self.current_tickers_set = set(self.ticker_cache[col].tolist())
                            break
                    else:
                        ticker_col = self.ticker_cache.columns[0]
                        self.current_tickers_set = set(self.ticker_cache[ticker_col].tolist())
            else:
                self.ticker_cache = pd.DataFrame(columns=[
                    "ticker", "name", "primary_exchange", "last_updated_utc", "type", "market", "locale"
                ])
                self.current_tickers_set = set()
            
            self.initial_refresh_complete.set()
            logger.info(f"Cache initialized with {len(self.current_tickers_set)} tickers")
        except Exception as e:
            logger.error(f"Failed to initialize cache: {e}")
            raise DatabaseError(f"Cache initialization failed: {e}")

    async def get_ticker_sector(self, ticker: str) -> Optional[str]:
        """Get sector information for a ticker - kept for compatibility"""
        return None  # Pure ranking doesn't use sectors

    async def get_ticker_sectors_bulk(self, tickers: List[str]) -> Dict[str, str]:
        """Get sector information for multiple tickers in bulk - kept for compatibility"""
        return {}  # Pure ranking doesn't use sectors

    @handle_errors()
    def is_trading_day(self, date):
        """Check if a date is a trading day using market calendar"""
        if isinstance(date, str):
            date = datetime.strptime(date, "%Y-%m-%d").date()
        elif isinstance(date, datetime):
            date = date.date()
            
        schedule = self.market_calendar.schedule(start_date=date, end_date=date)
        return not schedule.empty

    @handle_errors(max_retries=config.MAX_RETRIES, retry_delay=config.RETRY_DELAY)
    async def _call_polygon_api(self, session, url, retry_count=0):
        """Make API call with retry logic and rate limiting"""
        if self.shutdown_requested:
            return None
                
        if retry_count >= config.MAX_RETRIES:
            raise APIError(f"Max retries exceeded for URL: {url}")
            
        async with self.semaphore:
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        self.api_error_count = 0
                        return await response.json()
                    elif response.status == 429:
                        retry_after = int(response.headers.get('Retry-After', config.RETRY_DELAY))
                        await asyncio.sleep(retry_after)
                        return await self._call_polygon_api(session, url, retry_count+1)
                    elif response.status >= 500:
                        self.api_error_count += 1
                        await asyncio.sleep(config.RETRY_DELAY)
                        return await self._call_polygon_api(session, url, retry_count+1)
                    else:
                        self.api_error_count += 1
                        raise APIError(f"API request failed: {response.status} for URL: {url}")
            except asyncio.TimeoutError:
                self.api_error_count += 1
                await asyncio.sleep(config.RETRY_DELAY)
                return await self._call_polygon_api(session, url, retry_count+1)
            except aiohttp.ClientError as e:
                self.api_error_count += 1
                await asyncio.sleep(config.RETRY_DELAY)
                return await self._call_polygon_api(session, url, retry_count+1)
            except Exception as e:
                self.api_error_count += 1
                raise APIError(f"Unexpected error for URL {url}: {e}")

    @handle_errors()
    def _validate_ticker_data(self, ticker_data):
        """Validate ticker data before processing"""
        required_fields = ['ticker']
        validated_data = []
        
        for ticker in ticker_data:
            if not all(field in ticker for field in required_fields):
                continue
                
            sanitized = {
                'ticker': str(ticker.get('ticker', '')).strip(),
                'name': str(ticker.get('name', '')).strip() if ticker.get('name') else None,
                'primary_exchange': str(ticker.get('primary_exchange', '')).strip() if ticker.get('primary_exchange') else None,
                'last_updated_utc': str(ticker.get('last_updated_utc', '')).strip() if ticker.get('last_updated_utc') else None,
                'type': str(ticker.get('type', '')).strip() if ticker.get('type') else None,
                'market': str(ticker.get('market', '')).strip() if ticker.get('market') else None,
                'locale': str(ticker.get('locale', '')).strip() if ticker.get('locale') else None,
                'currency_name': str(ticker.get('currency_name', '')).strip() if ticker.get('currency_name') else None,
            }
            
            if not sanitized['ticker']:
                continue
                
            validated_data.append(sanitized)
        
        return validated_data

    @monitor_performance
    @handle_errors(max_retries=config.MAX_RETRIES, retry_delay=config.RETRY_DELAY)
    async def _fetch_composite_tickers(self, session, composite_index):
        """Fetch all tickers for a specific composite index"""
        logger.info(f"Fetching tickers for composite index {composite_index}")
        all_results = []
        next_url = None
        page_count = 0
        max_pages = 200
        
        date_param = datetime.now().strftime("%Y-%m-%d")
            
        if composite_index == "^IXAC":
            exchange = "XNAS"
        else:
            raise ConfigurationError(f"Unknown composite index: {composite_index}")
            
        params = {
            "market": "stocks",
            "exchange": exchange,
            "active": "true",
            "limit": 1000,
            "apiKey": self.api_key,
            "date": date_param
        }
        
        url = f"{self.base_url}?{urlencode(params)}"
        
        while url and page_count < max_pages and not self.shutdown_requested:
            data = await self._call_polygon_api(session, url)
            if not data or self.shutdown_requested:
                break
                
            results = data.get("results", [])
            if not results:
                break
                
            stock_results = [
                {**r, "composite_index": composite_index} 
                for r in results 
                if r.get('type', '').upper() == 'CS'
            ]
            all_results.extend(stock_results)
            
            next_url = data.get("next_url", None)
            url = f"{next_url}&apiKey={self.api_key}" if next_url else None
            page_count += 1
            
            delay = config.RATE_LIMIT_DELAY * (1 + page_count / 10)
            await asyncio.sleep(min(delay, 5.0))
        
        if self.shutdown_requested:
            return []
            
        logger.info(f"Completed {composite_index}: {len(all_results)} stocks across {page_count} pages")
        return all_results

    @monitor_performance
    @handle_errors(max_retries=config.MAX_RETRIES, retry_delay=config.RETRY_DELAY)
    async def _refresh_all_tickers_async(self):
        """Refresh all tickers with cleaner output"""
        start_time = time.time()
        
        logger.info("Refreshing ticker data")
        
        if self.shutdown_requested:
            return False
            
        try:
            async with aiohttp.ClientSession() as session:
                tasks = [self._fetch_composite_tickers(session, idx) for idx in self.composite_indices]
                composite_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                all_results = []
                for i, results in enumerate(composite_results):
                    if isinstance(results, Exception):
                        logger.error(f"Error fetching {self.composite_indices[i]}: {results}")
                        continue
                    if results:
                        all_results.extend(results)
                
                if self.shutdown_requested:
                    return False
        except Exception as e:
            logger.error(f"Error during API fetch: {e}")
            return await self._fallback_to_database()
        
        if not all_results:
            return await self._fallback_to_database()
            
        validated_results = self._validate_ticker_data(all_results)
        if not validated_results:
            return await self._fallback_to_database()
            
        new_df = pd.DataFrame(validated_results)[["ticker", "name", "primary_exchange", "last_updated_utc", "type", "market", "locale", "currency_name"]]
        new_tickers = set(new_df['ticker'].tolist())
        
        with self.cache_lock:
            old_tickers = set(self.current_tickers_set)
            added = new_tickers - old_tickers
            removed = old_tickers - new_tickers
            
            tickers_data = new_df.to_dict('records')
            inserted, updated = await self.db.upsert_tickers(tickers_data)
            
            if removed:
                marked_inactive = await self.db.mark_tickers_inactive(list(removed))
            
            self.ticker_cache = new_df
            self.current_tickers_set = new_tickers
        
        self.last_refresh_time = time.time()
            
        elapsed = time.time() - start_time
        
        # CRUCIAL: Log the ticker refresh summary as requested
        logger.info(f"Ticker refresh: {len(new_df)} total, {len(added)} new, {len(removed)} removed")
        
        # Log what was added and removed if needed
        if added:
            logger.debug(f"Added tickers: {sorted(list(added))}")
        if removed:
            logger.debug(f"Removed tickers: {sorted(list(removed))}")
            
        return True

    @handle_errors()
    async def _fallback_to_database(self):
        """Fallback to database if API fails"""
        logger.info("Attempting database fallback")
        
        with self.cache_lock:
            db_tickers = await self.db.get_all_active_tickers()
            if db_tickers:
                self.ticker_cache = pd.DataFrame(db_tickers)
                self.current_tickers_set = set(self.ticker_cache['ticker'].tolist())
                return True
            else:
                return False

    @monitor_performance
    @handle_errors(max_retries=config.MAX_RETRIES, retry_delay=config.RETRY_DELAY)
    async def refresh_all_tickers(self):
        """Public async method to refresh tickers"""
        with self.refresh_lock:
            return await self._refresh_all_tickers_async()

    def stop(self):
        self.active = False
        self.shutdown_requested = True

    @handle_errors()
    def get_current_tickers_list(self):
        with self.cache_lock:
            return self.ticker_cache['ticker'].tolist()

    @handle_errors()
    def get_ticker_details(self, ticker):
        with self.cache_lock:
            result = self.ticker_cache[self.ticker_cache['ticker'] == ticker]
            return result.to_dict('records')[0] if not result.empty else None

    async def initialize_combined_ranking_engine(self, config: Dict[str, Any] = None) -> CombinedRankingEngine:
        """Initialize the single combined ranking engine"""
        if self.combined_ranking_engine is None:
            self.combined_ranking_engine = CombinedRankingEngine(self, config)
        return self.combined_ranking_engine

    @monitor_performance
    @handle_errors()
    async def rank_all_tickers_and_save(self, max_tickers: int = None, strategy_config: Dict[str, Any] = None) -> Tuple[List[TickerRanking], str]:
        """Use combined engine for all ranking operations"""
        await self.initialize_combined_ranking_engine(strategy_config)
        
        rankings = await self.combined_ranking_engine.rank_all_tickers_optimized(max_tickers)
        csv_path = await self.combined_ranking_engine.save_rankings_to_csv(rankings)
        
        logger.info(f"Pure ranking completed: {len(rankings)} tickers ranked")
        
        return rankings, csv_path

    # ======================== TRADING INTEGRATION METHODS ======================== #
    
    async def initialize_trading_manager(self):
        """Initialize the Alpaca trading manager"""
        return await self.alpaca_manager.initialize()
    
    async def get_trading_capital(self) -> float:
        """Get available trading capital"""
        return await self.alpaca_manager.get_available_capital()
    
    async def has_existing_position(self, ticker: str) -> bool:
        """Check if we have an existing position for a ticker"""
        return await self.alpaca_manager.has_existing_position(ticker)
    
    async def get_account_status(self):
        """Get Alpaca account status"""
        return await self.alpaca_manager.display_account_status()
    
    async def get_portfolio_summary(self):
        """Get portfolio summary"""
        return await self.alpaca_manager.get_portfolio_summary()

    # ======================== TRADE EXECUTOR METHODS ======================== #
    
    async def initialize_trade_executor(self) -> TradeExecutor:
        """Initialize the trade executor"""
        if self.trade_executor is None:
            self.trade_executor = TradeExecutor(self)
            await self.trade_executor.start()
        return self.trade_executor

    # ======================== DISPLAY MANAGEMENT METHODS ======================== #
    
    async def display_comprehensive_status(self, rankings=None, performance_metrics=None, trade_signals=None):
        """Display comprehensive status using the unified display manager"""
        await self.initialize_combined_ranking_engine()
        
        self.display_manager.display_comprehensive_status(
            scanner=self,
            db_manager=self.db,
            task_scheduler=task_scheduler,
            rankings=rankings,
            performance_metrics=performance_metrics,
            trade_signals=trade_signals
        )

    async def quick_status(self):
        """Quick status display without detailed rankings"""
        self.display_manager.set_display_group('ranking_results', False)
        self.display_manager.set_display_group('performance', False)
        self.display_manager.set_display_group('trading_signals', False)
        
        await self.display_comprehensive_status()
        
        # Reset display groups to default
        for group in self.display_manager.display_groups:
            self.display_manager.display_groups[group] = True

    async def detailed_status(self, max_rankings=50):
        """Detailed status display with rankings"""
        rankings = await self.combined_ranking_engine.rank_all_tickers_optimized(max_rankings)
        await self.display_comprehensive_status(rankings=rankings[:max_rankings])

# ======================== SCHEDULER ======================== #
@monitor_performance
@handle_errors()
async def run_scheduled_ticker_refresh(scanner):
    """Run immediate scan on startup and then daily at scheduled time"""
    logger.info("Starting immediate ticker scan on startup")
    try:
        # STEP 1: DETECT CAPITAL AND POSITIONS FIRST
        task_scheduler.update_task_status('CAPITAL_DETECTION', 'running', datetime.now())
        capital_detected = await scanner.capital_detector.detect_capital_and_positions()
        if capital_detected:
            task_scheduler.update_task_status('CAPITAL_DETECTION', 'completed', datetime.now())
        else:
            task_scheduler.update_task_status('CAPITAL_DETECTION', 'failed', datetime.now())
        
        # STEP 2: Continue with existing pipeline
        success = await scanner.refresh_all_tickers() 
        if success:
            logger.info("Initial ticker scan completed successfully")
            
            # Update task status to running during processing
            task_scheduler.update_task_status('TICKER_REFRESH', 'running', datetime.now())
            
            max_tickers = config.MAX_TICKERS_TO_RANK
            logger.info(f"Starting automatic pure ticker ranking (max: {max_tickers if max_tickers is not None else 'ALL'} tickers)")
            try:
                # Update status to running for ranking tasks
                task_scheduler.update_task_status('RANKING_ANALYSIS', 'running', datetime.now())
                
                rankings, csv_path = await scanner.rank_all_tickers_and_save(max_tickers=max_tickers)
                if rankings:
                    # Update task status for completed tasks
                    task_scheduler.update_task_status('TICKER_REFRESH', 'completed', datetime.now())
                    task_scheduler.update_task_status('RANKING_ANALYSIS', 'completed', datetime.now())
                    
                    signal_counts = {}
                    for ranking in rankings:
                        signal_type = ranking.signal_type
                        signal_counts[signal_type] = signal_counts.get(signal_type, 0) + 1
                    
                    logger.info(f"Automatic pure ranking completed: {len(rankings)} tickers ranked, signals: {signal_counts}")
                    
                    # Display trading account status
                    await scanner.get_account_status()
                else:
                    logger.warning("Automatic pure ranking failed to generate results")
                    # Mark as failed
                    task_scheduler.update_task_status('RANKING_ANALYSIS', 'failed', datetime.now())
            except Exception as e:
                logger.error(f"Error during automatic pure ranking: {e}")
                # Mark as failed
                task_scheduler.update_task_status('RANKING_ANALYSIS', 'failed', datetime.now())
        else:
            logger.warning("Initial ticker scan encountered errors")
            task_scheduler.update_task_status('TICKER_REFRESH', 'failed', datetime.now())
    except asyncio.CancelledError:
        return
    except Exception as e:
        logger.error(f"Error during initial ticker scan: {e}")
        task_scheduler.update_task_status('TICKER_REFRESH', 'failed', datetime.now())
    
    # Display status after initial scan
    task_scheduler.display_status()
    
    while scanner.active and not scanner.shutdown_requested:
        now = datetime.now(scanner.local_tz)
        
        target_time = datetime.strptime(config.SCAN_TIME, "%H:%M").time()
        target_datetime = now.replace(
            hour=target_time.hour,
            minute=target_time.minute,
            second=0,
            microsecond=0
        )
        
        if now > target_datetime:
            target_datetime += timedelta(days=1)
        
        sleep_seconds = (target_datetime - now).total_seconds()
        hours = sleep_seconds // 3600
        minutes = (sleep_seconds % 3600) // 60
        
        while sleep_seconds > 0 and scanner.active and not scanner.shutdown_requested:
            try:
                await asyncio.sleep(min(1, sleep_seconds))
                sleep_seconds -= 1
            except asyncio.CancelledError:
                return
            
        if not scanner.active or scanner.shutdown_requested:
            break
            
        if not scanner.is_trading_day(datetime.now()):
            continue
            
        logger.info("Starting scheduled ticker refresh")
        try:
            # STEP 1: DETECT CAPITAL AND POSITIONS FIRST
            task_scheduler.update_task_status('CAPITAL_DETECTION', 'running', datetime.now())
            capital_detected = await scanner.capital_detector.detect_capital_and_positions()
            if capital_detected:
                task_scheduler.update_task_status('CAPITAL_DETECTION', 'completed', datetime.now())
            else:
                task_scheduler.update_task_status('CAPITAL_DETECTION', 'failed', datetime.now())
            
            # STEP 2: Continue with existing pipeline
            success = await scanner.refresh_all_tickers()
            if success:
                task_scheduler.update_task_status('TICKER_REFRESH', 'running', datetime.now())
                
                logger.info("Scheduled ticker refresh completed successfully")
                
                max_tickers = config.MAX_TICKERS_TO_RANK
                logger.info(f"Starting scheduled pure ticker ranking (max: {max_tickers if max_tickers is not None else 'ALL'} tickers)")
                try:
                    task_scheduler.update_task_status('RANKING_ANALYSIS', 'running', datetime.now())
                    
                    rankings, csv_path = await scanner.rank_all_tickers_and_save(max_tickers=max_tickers)
                    if rankings:
                        task_scheduler.update_task_status('TICKER_REFRESH', 'completed', datetime.now())
                        task_scheduler.update_task_status('RANKING_ANALYSIS', 'completed', datetime.now())
                        
                        signal_counts = {}
                        for ranking in rankings:
                            signal_type = ranking.signal_type
                            signal_counts[signal_type] = signal_counts.get(signal_type, 0) + 1
                        
                        logger.info(f"Scheduled pure ranking completed: {len(rankings)} tickers ranked, signals: {signal_counts}")
                        
                        # Display trading account status
                        await scanner.get_account_status()
                    else:
                        logger.warning("Scheduled pure ranking failed to generate results")
                        task_scheduler.update_task_status('RANKING_ANALYSIS', 'failed', datetime.now())
                except Exception as e:
                    logger.error(f"Error during scheduled pure ranking: {e}")
                    task_scheduler.update_task_status('RANKING_ANALYSIS', 'failed', datetime.now())
            else:
                logger.warning("Scheduled ticker refresh encountered errors")
                task_scheduler.update_task_status('TICKER_REFRESH', 'failed', datetime.now())
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.error(f"Error during scheduled ticker refresh: {e}")
            task_scheduler.update_task_status('TICKER_REFRESH', 'failed', datetime.now())
        
        task_scheduler.display_status()

# ======================== TRADING SCHEDULER ======================== #
async def run_scheduled_trading(scanner: PolygonTickerScanner):
    """Run scheduled trading tasks"""
    # Initialize trade executor
    trade_executor = await scanner.initialize_trade_executor()
    
    # Schedule trading tasks
    while scanner.active and not scanner.shutdown_requested:
        try:
            now = datetime.now()
            current_time = now.strftime("%H:%M")
            
            # Before market open: Generate and display pending executions
            if current_time == "08:45":  # 15 minutes before trade execution
                logger.info("🔄 Generating pending trade executions for market open")
                
                # Run ranking analysis
                rankings = await scanner.combined_ranking_engine.rank_all_tickers_optimized(50)
                
                # Generate pending executions
                await trade_executor.execute_trades_based_on_rankings(rankings, max_trades=10)
                
                # Update task status
                task_scheduler.update_task_status('TRADE_EXECUTION', 'running', now)
            
            # Market open: Execute pending trades
            elif current_time == config.TRADE_EXECUTION_TIME:
                logger.info("🏦 Executing pending trades at market open")
                await trade_executor.execute_pending_trades()
                task_scheduler.update_task_status('TRADE_EXECUTION', 'completed', now)
            
            # During market hours: Run periodic scans based on volatility
            elif "09:30" <= current_time <= "16:00":
                await trade_executor.run_scheduled_scan()
                # Wait for next scan interval
                scan_interval = trade_executor.volatility_monitor.get_scan_interval()
                await asyncio.sleep(scan_interval * 60)
            else:
                # Outside market hours, check every 5 minutes
                await asyncio.sleep(300)
                
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in scheduled trading: {e}")
            await asyncio.sleep(60)

# ======================== COMPREHENSIVE TRADING SYSTEM ======================== #
async def run_comprehensive_trading_system():
    """Run the complete trading system with execution capabilities"""
    scanner = PolygonTickerScanner()
    
    try:
        # Start the scanner
        await scanner.start()
        
        # Start capital detection first
        await scanner.capital_detector.detect_capital_and_positions()
        
        # Initialize trading system
        trade_executor = await scanner.initialize_trade_executor()
        
        # Run both scanning and trading in parallel
        await asyncio.gather(
            run_scheduled_ticker_refresh(scanner),
            run_scheduled_trading(scanner),
            return_exceptions=True
        )
        
    except KeyboardInterrupt:
        logger.info("Trading system stopped by user")
    except Exception as e:
        logger.error(f"Trading system error: {e}")
    finally:
        await scanner.shutdown()
        if hasattr(scanner, 'trade_executor'):
            await scanner.trade_executor.stop()

# ======================== SIMPLIFIED EXECUTION ======================== #
async def run_scanner_with_display():
    """Run the scanner with unified display output"""
    ticker_scanner = PolygonTickerScanner()
    
    try:
        await ticker_scanner.start()
        await asyncio.get_event_loop().run_in_executor(None, ticker_scanner.initial_refresh_complete.wait)
        
        await run_scheduled_ticker_refresh(ticker_scanner)
        
    except KeyboardInterrupt:
        logger.info("Scanner stopped by user")
    except Exception as e:
        logger.error(f"Scanner error: {e}")
    finally:
        await ticker_scanner.shutdown()

# ======================== QUICK ACCESS FUNCTIONS ======================== #
async def quick_status():
    """Quick status check - can be called independently"""
    scanner = PolygonTickerScanner()
    try:
        await scanner.start()
        await scanner.quick_status()
    finally:
        await scanner.shutdown()

async def full_scan_and_display(max_tickers=50):
    """Run full scan and display comprehensive results"""
    scanner = PolygonTickerScanner()
    try:
        await scanner.start()
        rankings, csv_path = await scanner.rank_all_tickers_and_save(max_tickers=max_tickers)
        await scanner.display_comprehensive_status(rankings=rankings)
        return rankings, csv_path
    finally:
        await scanner.shutdown()

async def trading_status():
    """Display trading account status only"""
    scanner = PolygonTickerScanner()
    try:
        await scanner.start()
        await scanner.get_account_status()
    finally:
        await scanner.shutdown()

async def capital_detection_only():
    """Run capital and position detection only"""
    scanner = PolygonTickerScanner()
    try:
        await scanner.start()
        # Just initialize the trading manager and run capital detection
        await scanner.alpaca_manager.initialize()
        capital_detector = CapitalPositionDetector(scanner.alpaca_manager)
        await capital_detector.detect_capital_and_positions()
    finally:
        await scanner.shutdown()

def main():
    """Main entry point with unified display"""
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    try:
        asyncio.run(run_comprehensive_trading_system())
    except KeyboardInterrupt:
        logger.info("Application terminated by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
