# backtester.py
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import asyncio
from typing import List, Dict, Optional, Tuple
import json
from dataclasses import dataclass
import matplotlib.pyplot as plt
import seaborn as sns
from enum import Enum

# Import your existing classes
from long_algo import (
    MomentumMeanReversionStrategy, TickerRanking, SignalType, 
    SectorMappingManager, CombinedRankingEngine, PolygonTickerScanner
)

# ======================== BACKTEST CONFIGURATION ======================== #
class BacktestConfig:
    """Configuration for backtesting"""
    def __init__(self):
        # Historical data paths
        self.historical_data_dir = r"C:\Users\kyung\StockScanner\historical_data"
        self.historical_tickers_dir = os.path.join(self.historical_data_dir, "historical_tickers")
        self.backtest_sectors_dir = os.path.join(self.historical_data_dir, "backtest_sectors")
        
        # Backtest parameters
        self.start_date = "2020-01-01"
        self.end_date = "2023-12-31"
        self.initial_capital = 100000.0
        self.max_positions = 10
        self.max_position_size = 0.1  # 10% per position
        self.max_sector_allocation = 0.25  # 25% per sector
        self.commission = 0.001  # 0.1% commission
        self.slippage = 0.001  # 0.1% slippage
        
        # Strategy parameters (matching your live config)
        self.ranking_engine_config = {
            'trend_ema_fast': 13,
            'trend_ema_slow': 34,
            'trend_adx_period': 21,
            'mean_reversion_rsi_period': 21,
            'bollinger_period': 26,
            'momentum_period': 14,
            'weight_trend_strength': 0.28,
            'weight_mean_reversion': 0.25,
            'weight_volume': 0.25,
            'weight_momentum': 0.17,
            'weight_volatility': 0.05
        }

# ======================== BACKTEST DATA MANAGER ======================== #
class BacktestDataManager:
    """Manages historical data for backtesting"""
    
    def __init__(self, config: BacktestConfig):
        self.config = config
        self.ticker_data = {}
        self.sector_data = {}
        self.available_dates = set()
        self._load_data()
    
    def _load_data(self):
        """Load all historical data"""
        print("Loading historical data for backtest...")
        
        # Load sector data
        self._load_sector_data()
        
        # Load ticker data
        self._load_ticker_data()
        
        # Extract available dates
        self._extract_available_dates()
        
        print(f"✅ Loaded {len(self.ticker_data)} tickers with data from {min(self.available_dates)} to {max(self.available_dates)}")
    
    def _load_sector_data(self):
        """Load sector mapping from backtest_sectors directory"""
        sector_files = {
            "Technology": "Technology.csv",
            "Health Care": "Health Care.csv", 
            "Financials": "Financials.csv",
            "Consumer Discretionary": "Consumer Discretionary.csv",
            "Consumer Staples": "Consumer Staples.csv",
            "Industrials": "Industrials.csv",
            "Energy": "Energy.csv",
            "Utilities": "Utilities.csv",
            "Real Estate": "Real Estate.csv",
            "Materials": "Materials.csv",
            "Communication Services": "Communication Services.csv",
            "Other": "Other.csv"
        }
        
        for sector, filename in sector_files.items():
            filepath = os.path.join(self.config.backtest_sectors_dir, filename)
            if os.path.exists(filepath):
                try:
                    df = pd.read_csv(filepath)
                    # Assuming the CSV has a 'Ticker' column
                    if 'Ticker' in df.columns:
                        self.sector_data[sector] = set(df['Ticker'].tolist())
                    elif 'ticker' in df.columns:
                        self.sector_data[sector] = set(df['ticker'].tolist())
                    else:
                        # Use first column as ticker
                        self.sector_data[sector] = set(df.iloc[:, 0].tolist())
                except Exception as e:
                    print(f"Warning: Could not load sector file {filename}: {e}")
    
    def _load_ticker_data(self):
        """Load individual ticker data from historical_tickers directory"""
        ticker_files = [f for f in os.listdir(self.config.historical_tickers_dir) 
                       if f.endswith('.csv')]
        
        for filename in ticker_files:
            ticker = filename.replace('.csv', '')
            filepath = os.path.join(self.config.historical_tickers_dir, filename)
            
            try:
                df = pd.read_csv(filepath)
                
                # Convert date column
                if 'Date' in df.columns:
                    df['Date'] = pd.to_datetime(df['Date'])
                    df.set_index('Date', inplace=True)
                elif 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                    df.set_index('date', inplace=True)
                
                # Ensure we have required columns
                required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
                if all(col in df.columns for col in required_cols):
                    # Sort by date and ensure no duplicates
                    df = df.sort_index()
                    df = df[~df.index.duplicated(keep='first')]
                    
                    self.ticker_data[ticker] = df
                    
            except Exception as e:
                print(f"Warning: Could not load ticker data for {ticker}: {e}")
    
    def _extract_available_dates(self):
        """Extract all available trading dates from the data"""
        all_dates = set()
        for ticker, df in self.ticker_data.items():
            all_dates.update(df.index.date)
        
        self.available_dates = sorted(all_dates)
    
    def get_ticker_data(self, ticker: str, date: datetime) -> Optional[pd.DataFrame]:
        """Get historical data for a ticker up to a specific date"""
        if ticker not in self.ticker_data:
            return None
        
        df = self.ticker_data[ticker]
        data_up_to_date = df[df.index.date <= date.date()]
        
        if len(data_up_to_date) < 30:  # Need minimum data for indicators
            return None
        
        return data_up_to_date
    
    def get_price(self, ticker: str, date: datetime) -> Optional[float]:
        """Get the closing price for a ticker on a specific date"""
        if ticker not in self.ticker_data:
            return None
        
        df = self.ticker_data[ticker]
        date_data = df[df.index.date == date.date()]
        
        if len(date_data) == 0:
            return None
        
        return float(date_data['Close'].iloc[0])
    
    def get_ticker_sector(self, ticker: str) -> str:
        """Get the sector for a ticker"""
        for sector, tickers in self.sector_data.items():
            if ticker in tickers:
                return sector
        return "Other"
    
    def get_available_tickers(self, date: datetime) -> List[str]:
        """Get all tickers that have data on a specific date"""
        available_tickers = []
        for ticker, df in self.ticker_data.items():
            if date.date() in df.index.date:
                available_tickers.append(ticker)
        return available_tickers

# ======================== BACKTEST PORTFOLIO ======================== #
@dataclass
class Position:
    """Represents a trading position in the backtest"""
    ticker: str
    quantity: int
    entry_price: float
    entry_date: datetime
    sector: str
    
    @property
    def market_value(self, current_price: float) -> float:
        return self.quantity * current_price
    
    def unrealized_pnl(self, current_price: float) -> float:
        return (current_price - self.entry_price) * self.quantity

class BacktestPortfolio:
    """Manages portfolio during backtesting"""
    
    def __init__(self, initial_capital: float):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.positions: Dict[str, Position] = {}
        self.transaction_history = []
        self.daily_values = []
        self.sector_exposure = {}
        
    def add_position(self, ticker: str, quantity: int, price: float, date: datetime, sector: str, commission: float = 0.0):
        """Add a new position to the portfolio"""
        cost = quantity * price + commission
        
        if cost > self.cash:
            raise ValueError(f"Insufficient cash: {cost} > {self.cash}")
        
        if ticker in self.positions:
            # Update existing position
            old_pos = self.positions[ticker]
            total_quantity = old_pos.quantity + quantity
            avg_price = ((old_pos.quantity * old_pos.entry_price) + (quantity * price)) / total_quantity
            
            self.positions[ticker] = Position(
                ticker=ticker,
                quantity=total_quantity,
                entry_price=avg_price,
                entry_date=date,
                sector=sector
            )
        else:
            # New position
            self.positions[ticker] = Position(
                ticker=ticker,
                quantity=quantity,
                entry_price=price,
                entry_date=date,
                sector=sector
            )
        
        self.cash -= cost
        
        # Record transaction
        self.transaction_history.append({
            'date': date,
            'ticker': ticker,
            'action': 'BUY',
            'quantity': quantity,
            'price': price,
            'commission': commission,
            'total_cost': cost
        })
    
    def remove_position(self, ticker: str, price: float, date: datetime, commission: float = 0.0):
        """Remove a position from the portfolio"""
        if ticker not in self.positions:
            return 0
        
        position = self.positions[ticker]
        proceeds = position.quantity * price - commission
        pnl = (price - position.entry_price) * position.quantity
        
        self.cash += proceeds
        del self.positions[ticker]
        
        # Record transaction
        self.transaction_history.append({
            'date': date,
            'ticker': ticker,
            'action': 'SELL',
            'quantity': position.quantity,
            'price': price,
            'commission': commission,
            'total_proceeds': proceeds,
            'realized_pnl': pnl
        })
        
        return pnl
    
    def get_position_quantity(self, ticker: str) -> int:
        """Get quantity of a position"""
        if ticker in self.positions:
            return self.positions[ticker].quantity
        return 0
    
    def calculate_portfolio_value(self, price_getter) -> float:
        """Calculate total portfolio value"""
        total_value = self.cash
        
        for ticker, position in self.positions.items():
            current_price = price_getter(ticker)
            if current_price is not None:
                total_value += position.market_value(current_price)
        
        return total_value
    
    def calculate_sector_allocation(self, price_getter) -> Dict[str, float]:
        """Calculate current sector allocation"""
        total_value = self.calculate_portfolio_value(price_getter)
        if total_value == 0:
            return {}
        
        sector_values = {}
        for position in self.positions.values():
            current_price = price_getter(position.ticker)
            if current_price is not None:
                position_value = position.market_value(current_price)
                sector_values[position.sector] = sector_values.get(position.sector, 0) + position_value
        
        # Convert to percentages
        sector_allocation = {sector: value / total_value for sector, value in sector_values.items()}
        return sector_allocation
    
    def record_daily_value(self, date: datetime, total_value: float):
        """Record daily portfolio value"""
        self.daily_values.append({
            'date': date,
            'total_value': total_value,
            'cash': self.cash,
            'positions_count': len(self.positions)
        })

# ======================== BACKTEST ENGINE ======================== #
class BacktestEngine:
    """Main backtesting engine"""
    
    def __init__(self, config: BacktestConfig):
        self.config = config
        self.data_manager = BacktestDataManager(config)
        self.portfolio = BacktestPortfolio(config.initial_capital)
        self.strategy = MomentumMeanReversionStrategy(config.ranking_engine_config)
        self.sector_manager = SectorMappingManager()
        
        # Results storage
        self.ranking_history = {}
        self.trade_signals_history = []
        self.performance_metrics = {}
        
    def run_backtest(self):
        """Run the complete backtest"""
        print("🚀 Starting backtest...")
        
        # Filter dates within backtest range
        start_date = datetime.strptime(self.config.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(self.config.end_date, "%Y-%m-%d").date()
        
        test_dates = [date for date in self.data_manager.available_dates 
                     if start_date <= date <= end_date]
        
        print(f"Backtesting from {start_date} to {end_date} ({len(test_dates)} trading days)")
        
        for i, current_date in enumerate(test_dates):
            if i % 100 == 0:
                print(f"Processing date {i+1}/{len(test_dates)}: {current_date}")
            
            # Convert to datetime for compatibility
            current_datetime = datetime.combine(current_date, datetime.min.time())
            
            # Step 1: Run ranking analysis for this date
            rankings = self._run_ranking_for_date(current_datetime)
            
            # Step 2: Generate trade signals
            signals = self._generate_trade_signals(rankings, current_datetime)
            
            # Step 3: Execute trades
            self._execute_trades(signals, current_datetime)
            
            # Step 4: Record portfolio value
            total_value = self.portfolio.calculate_portfolio_value(
                lambda ticker: self.data_manager.get_price(ticker, current_datetime)
            )
            self.portfolio.record_daily_value(current_datetime, total_value)
            
            # Store ranking history for analysis
            self.ranking_history[current_date] = rankings
        
        print("✅ Backtest completed!")
        
        # Calculate performance metrics
        self._calculate_performance_metrics()
        
        return self._get_backtest_results()
    
    def _run_ranking_for_date(self, date: datetime) -> List[TickerRanking]:
        """Run ranking analysis for a specific date"""
        rankings = []
        available_tickers = self.data_manager.get_available_tickers(date)
        
        for ticker in available_tickers[:200]:  # Limit for performance
            historical_data = self.data_manager.get_ticker_data(ticker, date)
            if historical_data is None:
                continue
            
            # Use your strategy to calculate ranking
            ranking = self.strategy.calculate_ticker_ranking_optimized(ticker, historical_data)
            if ranking is not None:
                # Add sector information
                ranking.sector = self.data_manager.get_ticker_sector(ticker)
                ranking.detailed_sector = ranking.sector
                rankings.append(ranking)
        
        # Sort by total score
        rankings.sort(key=lambda x: x.total_score, reverse=True)
        
        # Assign ranks
        for i, ranking in enumerate(rankings):
            ranking.rank = i + 1
        
        return rankings
    
    def _generate_trade_signals(self, rankings: List[TickerRanking], date: datetime) -> List[Dict]:
        """Generate trade signals based on rankings and current portfolio"""
        signals = []
        current_sector_allocation = self.portfolio.calculate_sector_allocation(
            lambda ticker: self.data_manager.get_price(ticker, date)
        )
        
        for ranking in rankings[:50]:  # Consider top 50 ranked tickers
            if ranking.total_score < 0.4:  # Minimum score threshold
                continue
            
            # Check if we already have a position
            current_quantity = self.portfolio.get_position_quantity(ranking.ticker)
            if current_quantity > 0:
                continue
            
            # Check sector allocation
            sector_allocation = current_sector_allocation.get(ranking.sector, 0)
            if sector_allocation >= self.config.max_sector_allocation:
                continue
            
            # Calculate position size
            available_cash = self.portfolio.cash
            position_value = min(
                available_cash * self.config.max_position_size,
                self.portfolio.calculate_portfolio_value(
                    lambda ticker: self.data_manager.get_price(ticker, date)
                ) * self.config.max_position_size
            )
            
            current_price = self.data_manager.get_price(ranking.ticker, date)
            if current_price is None:
                continue
            
            quantity = int(position_value / current_price)
            if quantity < 1:
                continue
            
            # Only take LONG signals for backtest
            if ranking.signal_type == "LONG":
                signals.append({
                    'ticker': ranking.ticker,
                    'signal_type': 'BUY',
                    'quantity': quantity,
                    'price': current_price,
                    'sector': ranking.sector,
                    'ranking_score': ranking.total_score,
                    'confidence': ranking.confidence
                })
        
        return signals
    
    def _execute_trades(self, signals: List[Dict], date: datetime):
        """Execute trade signals"""
        # First, check if we need to close any positions (simple exit strategy)
        self._evaluate_exits(date)
        
        # Then execute new signals
        for signal in signals:
            if len(self.portfolio.positions) >= self.config.max_positions:
                break
            
            # Check sector constraints again (in case previous trades changed allocation)
            current_sector_allocation = self.portfolio.calculate_sector_allocation(
                lambda ticker: self.data_manager.get_price(ticker, date)
            )
            sector_allocation = current_sector_allocation.get(signal['sector'], 0)
            
            # Calculate what this trade would add to sector allocation
            trade_value = signal['quantity'] * signal['price']
            total_portfolio_value = self.portfolio.calculate_portfolio_value(
                lambda ticker: self.data_manager.get_price(ticker, date)
            )
            proposed_addition = trade_value / total_portfolio_value if total_portfolio_value > 0 else 0
            
            if sector_allocation + proposed_addition > self.config.max_sector_allocation:
                continue
            
            # Execute buy order with commission and slippage
            execution_price = signal['price'] * (1 + self.config.slippage)
            commission = execution_price * signal['quantity'] * self.config.commission
            
            try:
                self.portfolio.add_position(
                    ticker=signal['ticker'],
                    quantity=signal['quantity'],
                    price=execution_price,
                    date=date,
                    sector=signal['sector'],
                    commission=commission
                )
                
                # Record trade signal
                self.trade_signals_history.append({
                    'date': date,
                    'signal': signal,
                    'executed': True,
                    'execution_price': execution_price
                })
                
            except ValueError as e:
                # Insufficient cash
                self.trade_signals_history.append({
                    'date': date,
                    'signal': signal,
                    'executed': False,
                    'reason': str(e)
                })
    
    def _evaluate_exits(self, date: datetime):
        """Evaluate existing positions for exits"""
        positions_to_close = []
        
        for ticker, position in self.portfolio.positions.items():
            current_price = self.data_manager.get_price(ticker, date)
            if current_price is None:
                continue
            
            # Simple exit strategy: 20% stop loss or 50% take profit
            pnl_pct = (current_price - position.entry_price) / position.entry_price
            
            if pnl_pct <= -0.20:  # 20% stop loss
                positions_to_close.append((ticker, current_price, 'Stop Loss'))
            elif pnl_pct >= 0.50:  # 50% take profit
                positions_to_close.append((ticker, current_price, 'Take Profit'))
        
        # Close positions
        for ticker, price, reason in positions_to_close:
            commission = price * self.portfolio.positions[ticker].quantity * self.config.commission
            self.portfolio.remove_position(ticker, price, date, commission)
            
            print(f"Closed position: {ticker} at {price:.2f} ({reason})")
    
    def _calculate_performance_metrics(self):
        """Calculate comprehensive performance metrics"""
        if not self.portfolio.daily_values:
            return
        
        daily_values_df = pd.DataFrame(self.portfolio.daily_values)
        daily_values_df.set_index('date', inplace=True)
        
        # Basic metrics
        initial_value = self.config.initial_capital
        final_value = daily_values_df['total_value'].iloc[-1]
        total_return = (final_value - initial_value) / initial_value
        
        # Calculate daily returns
        daily_values_df['daily_return'] = daily_values_df['total_value'].pct_change()
        
        # Remove first row with NaN
        daily_returns = daily_values_df['daily_return'].dropna()
        
        # Performance metrics
        self.performance_metrics = {
            'initial_capital': initial_value,
            'final_value': final_value,
            'total_return': total_return,
            'total_return_pct': total_return * 100,
            'annualized_return': (1 + total_return) ** (252 / len(daily_returns)) - 1,
            'volatility': daily_returns.std() * np.sqrt(252),
            'sharpe_ratio': (daily_returns.mean() * 252) / (daily_returns.std() * np.sqrt(252)) if daily_returns.std() > 0 else 0,
            'max_drawdown': self._calculate_max_drawdown(daily_values_df['total_value']),
            'total_trades': len(self.portfolio.transaction_history),
            'winning_trades': len([t for t in self.portfolio.transaction_history if t.get('realized_pnl', 0) > 0]),
            'losing_trades': len([t for t in self.portfolio.transaction_history if t.get('realized_pnl', 0) < 0]),
            'avg_trade_return': np.mean([t.get('realized_pnl', 0) for t in self.portfolio.transaction_history if 'realized_pnl' in t]) if any('realized_pnl' in t for t in self.portfolio.transaction_history) else 0
        }
    
    def _calculate_max_drawdown(self, portfolio_values: pd.Series) -> float:
        """Calculate maximum drawdown"""
        peak = portfolio_values.expanding().max()
        drawdown = (portfolio_values - peak) / peak
        return drawdown.min()
    
    def _get_backtest_results(self) -> Dict:
        """Compile comprehensive backtest results"""
        return {
            'performance_metrics': self.performance_metrics,
            'portfolio_history': self.portfolio.daily_values,
            'transaction_history': self.portfolio.transaction_history,
            'trade_signals_history': self.trade_signals_history,
            'ranking_history': self.ranking_history
        }

# ======================== RESULTS VISUALIZATION ======================== #
class BacktestVisualizer:
    """Visualizes backtest results"""
    
    @staticmethod
    def plot_performance(results: Dict, save_path: str = None):
        """Plot portfolio performance"""
        portfolio_history = pd.DataFrame(results['portfolio_history'])
        portfolio_history.set_index('date', inplace=True)
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        
        # Portfolio value over time
        axes[0, 0].plot(portfolio_history.index, portfolio_history['total_value'])
        axes[0, 0].set_title('Portfolio Value Over Time')
        axes[0, 0].set_ylabel('Portfolio Value ($)')
        axes[0, 0].grid(True)
        
        # Daily returns
        portfolio_history['daily_return'] = portfolio_history['total_value'].pct_change()
        axes[0, 1].hist(portfolio_history['daily_return'].dropna(), bins=50, alpha=0.7)
        axes[0, 1].set_title('Distribution of Daily Returns')
        axes[0, 1].set_xlabel('Daily Return')
        axes[0, 1].set_ylabel('Frequency')
        
        # Drawdown
        portfolio_history['peak'] = portfolio_history['total_value'].expanding().max()
        portfolio_history['drawdown'] = (portfolio_history['total_value'] - portfolio_history['peak']) / portfolio_history['peak']
        axes[1, 0].plot(portfolio_history.index, portfolio_history['drawdown'] * 100)
        axes[1, 0].set_title('Portfolio Drawdown')
        axes[1, 0].set_ylabel('Drawdown (%)')
        axes[1, 0].grid(True)
        
        # Positions count
        axes[1, 1].plot(portfolio_history.index, portfolio_history['positions_count'])
        axes[1, 1].set_title('Number of Positions Over Time')
        axes[1, 1].set_ylabel('Number of Positions')
        axes[1, 1].grid(True)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        
        plt.show()
    
    @staticmethod
    def print_performance_summary(results: Dict):
        """Print performance summary"""
        metrics = results['performance_metrics']
        
        print("\n" + "="*60)
        print("BACKTEST PERFORMANCE SUMMARY")
        print("="*60)
        print(f"Initial Capital: ${metrics['initial_capital']:,.2f}")
        print(f"Final Value: ${metrics['final_value']:,.2f}")
        print(f"Total Return: {metrics['total_return_pct']:+.2f}%")
        print(f"Annualized Return: {metrics['annualized_return']*100:+.2f}%")
        print(f"Volatility: {metrics['volatility']*100:.2f}%")
        print(f"Sharpe Ratio: {metrics['sharpe_ratio']:.2f}")
        print(f"Max Drawdown: {metrics['max_drawdown']*100:.2f}%")
        print(f"Total Trades: {metrics['total_trades']}")
        print(f"Winning Trades: {metrics['winning_trades']}")
        print(f"Losing Trades: {metrics['losing_trades']}")
        print(f"Win Rate: {metrics['winning_trades']/metrics['total_trades']*100:.1f}%" if metrics['total_trades'] > 0 else "Win Rate: N/A")
        print(f"Average Trade Return: ${metrics['avg_trade_return']:.2f}")
        print("="*60)

# ======================== MAIN BACKTEST FUNCTION ======================== #
async def run_comprehensive_backtest():
    """Run a comprehensive backtest"""
    print("Starting Comprehensive Backtest...")
    
    # Configuration
    config = BacktestConfig()
    
    # Initialize and run backtest
    backtest_engine = BacktestEngine(config)
    results = backtest_engine.run_backtest()
    
    # Display results
    BacktestVisualizer.print_performance_summary(results)
    BacktestVisualizer.plot_performance(results, "backtest_performance.png")
    
    # Save detailed results
    with open('backtest_results.json', 'w') as f:
        # Convert datetime objects to strings for JSON serialization
        serializable_results = {
            'performance_metrics': results['performance_metrics'],
            'portfolio_history': [
                {**item, 'date': item['date'].isoformat()} 
                for item in results['portfolio_history']
            ],
            'transaction_history': [
                {**item, 'date': item['date'].isoformat()} 
                for item in results['transaction_history']
            ],
            'trade_signals_history': [
                {**item, 'date': item['date'].isoformat()} 
                for item in results['trade_signals_history']
            ]
        }
        json.dump(serializable_results, f, indent=2)
    
    print("Backtest results saved to 'backtest_results.json'")
    print("Performance chart saved to 'backtest_performance.png'")
    
    return results

# ======================== SECTOR ANALYSIS ======================== #
def analyze_sector_performance(results: Dict):
    """Analyze performance by sector"""
    transactions = pd.DataFrame(results['transaction_history'])
    
    if transactions.empty:
        print("No transactions to analyze")
        return
    
    # Convert date strings back to datetime if needed
    if 'date' in transactions.columns and isinstance(transactions['date'].iloc[0], str):
        transactions['date'] = pd.to_datetime(transactions['date'])
    
    # Calculate sector P&L
    sector_pnl = transactions.groupby('sector')['realized_pnl'].sum().sort_values(ascending=False)
    
    print("\nSECTOR PERFORMANCE ANALYSIS")
    print("="*40)
    for sector, pnl in sector_pnl.items():
        print(f"{sector:25} | ${pnl:>10.2f} | {pnl/results['performance_metrics']['initial_capital']*100:>6.1f}%")
    
    # Plot sector performance
    plt.figure(figsize=(12, 6))
    sector_pnl.plot(kind='bar')
    plt.title('Realized P&L by Sector')
    plt.ylabel('P&L ($)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# ======================== PARAMETER OPTIMIZATION ======================== #
def optimize_strategy_parameters():
    """Simple parameter optimization for the strategy"""
    print("Running parameter optimization...")
    
    # Define parameter ranges to test
    parameter_sets = [
        {'weight_trend_strength': 0.30, 'weight_mean_reversion': 0.25, 'weight_momentum': 0.15},
        {'weight_trend_strength': 0.25, 'weight_mean_reversion': 0.30, 'weight_momentum': 0.15},
        {'weight_trend_strength': 0.28, 'weight_mean_reversion': 0.25, 'weight_momentum': 0.17},
        {'weight_trend_strength': 0.20, 'weight_mean_reversion': 0.25, 'weight_momentum': 0.25},
    ]
    
    best_sharpe = -float('inf')
    best_params = None
    best_results = None
    
    for i, params in enumerate(parameter_sets):
        print(f"Testing parameter set {i+1}/{len(parameter_sets)}: {params}")
        
        config = BacktestConfig()
        config.ranking_engine_config.update(params)
        
        # Run shorter backtest for optimization
        config.start_date = "2022-01-01"
        config.end_date = "2023-06-30"
        
        backtest_engine = BacktestEngine(config)
        results = backtest_engine.run_backtest()
        
        sharpe = results['performance_metrics']['sharpe_ratio']
        
        if sharpe > best_sharpe:
            best_sharpe = sharpe
            best_params = params
            best_results = results
    
    print(f"\n🎯 Best Parameters: {best_params}")
    print(f"Best Sharpe Ratio: {best_sharpe:.3f}")
    
    return best_params, best_results

if __name__ == "__main__":
    # Run the backtest
    results = asyncio.run(run_comprehensive_backtest())
    
    # Additional analysis
    analyze_sector_performance(results)
    
    # Uncomment to run parameter optimization
    # best_params, optimization_results = optimize_strategy_parameters()