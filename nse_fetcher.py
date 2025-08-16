# nse_fetcher.py - Main fetcher module
import asyncio
import aiohttp
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging
import backoff
import os
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NSEOptionsFetcher:
    """Production NSE Options Fetcher for GitHub Actions"""
    
    def __init__(self, db_path="data/nse_options.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        self.headers = {
            "accept": "application/json, text/plain, */*",
            "referer": "https://www.nseindia.com/option-chain",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "x-requested-with": "XMLHttpRequest"
        }
        
        # Load stock list from config
        self.load_config()
        self._init_database()
        self.session = None
        self.semaphore = asyncio.Semaphore(3)
        
    def load_config(self):
        """Load configuration"""
        # Default NIFTY50 stocks
        self.nifty50_stocks = [
            "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK",
            "SBIN", "BHARTIARTL", "KOTAKBANK", "ITC", "AXISBANK",
            "LT", "BAJFINANCE", "HDFC", "MARUTI", "HINDUNILVR",
            "ASIANPAINT", "HCLTECH", "WIPRO", "TATAMOTORS", "SUNPHARMA",
            "TITAN", "ULTRACEMCO", "NESTLEIND", "ONGC", "POWERGRID",
            "NTPC", "M&M", "BAJAJFINSV", "TATASTEEL", "INDUSINDBK",
            "TECHM", "JSWSTEEL", "ADANIENT", "GRASIM", "DIVISLAB",
            "CIPLA", "DRREDDY", "COALINDIA", "BRITANNIA", "EICHERMOT",
            "HINDALCO", "ADANIPORTS", "SHREECEM", "UPL", "VEDL",
            "BPCL", "HEROMOTOCO", "TATACONSUM", "SBILIFE", "BAJAJ-AUTO"
        ]
        self.years_to_check = config.get('years', [2022, 2023, 2024, 2025])
        
        # Check for config file
        if os.path.exists('config.json'):
            with open('config.json', 'r') as f:
                config = json.load(f)
                self.nifty50_stocks = config.get('stocks', self.nifty50_stocks)
        
    def _init_database(self):
        """Initialize database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS option_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                expiry_date TEXT NOT NULL,
                strike_price REAL NOT NULL,
                option_type TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                ltp REAL,
                prev_close REAL,
                settle_price REAL,
                volume INTEGER,
                turnover REAL,
                open_interest INTEGER,
                change_in_oi INTEGER,
                market_lot INTEGER,
                underlying_value REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, expiry_date, strike_price, option_type, timestamp)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fetch_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                run_type TEXT,
                symbol TEXT,
                status TEXT,
                records_fetched INTEGER,
                error_message TEXT
            )
        ''')
        
        # Create indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_symbol_date ON option_data(symbol, timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_volume ON option_data(symbol, volume DESC)')
        
        conn.commit()
        conn.close()
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(headers=self.headers)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    @backoff.on_exception(backoff.expo, (aiohttp.ClientError, asyncio.TimeoutError), max_tries=3)
    async def fetch_url(self, url: str) -> Optional[Dict]:
        """Fetch URL with retry"""
        async with self.semaphore:
            try:
                async with self.session.get(url, timeout=30) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        logger.warning(f"HTTP {response.status} for {url}")
                        return None
            except Exception as e:
                logger.error(f"Error fetching {url}: {str(e)}")
                raise
    
    def get_month_number(self, month_str: str) -> str:
        """Convert month name to number"""
        months = {
            'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04',
            'MAY': '05', 'JUN': '06', 'JUL': '07', 'AUG': '08',
            'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'
        }
        return months.get(month_str.upper(), '01')
    
    async def fetch_stock_historical(self, symbol: str, days: int = 90) -> int:
        """Fetch historical data for a stock"""
        logger.info(f"Fetching {symbol} - Last {days} days")
        
        try:
            # Get years from config or default to last 3 years
            if hasattr(self, 'years_to_check'):
                years_to_check = self.years_to_check
            else:
                current_year = datetime.now().year
                years_to_check = [current_year - 3, current_year - 2, current_year - 1, current_year]  # Last 4 years including current
            
            all_expiries = []
            
            # Get all expiries
            for year in years_to_check:
                url = f"https://www.nseindia.com/api/historicalOR/meta/foCPV/expireDts?instrument=OPTSTK&symbol={symbol}&year={year}"
                data = await self.fetch_url(url)
                
                if data and 'expiresDts' in data:
                    all_expiries.extend([(exp, year) for exp in data['expiresDts']])
            
            if not all_expiries:
                logger.warning(f"No expiries found for {symbol}")
                self.log_fetch('historical', symbol, 'NO_EXPIRIES', 0)
                return 0
            
            # Filter expiries within our date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            relevant_expiries = []
            for expiry_str, year in all_expiries:
                try:
                    expiry_date = datetime.strptime(expiry_str, "%d-%b-%Y")
                    if expiry_date >= start_date:
                        relevant_expiries.append((expiry_str, year, expiry_date))
                except:
                    pass
            
            logger.info(f"Found {len(relevant_expiries)} relevant expiries for {symbol}")
            
            total_records = 0
            
            for expiry_str, year, expiry_date in relevant_expiries[:3]:  # Process max 3 expiries
                logger.info(f"  Processing expiry: {expiry_str}")
                
                # Calculate date range
                fetch_end = min(expiry_date, end_date)
                fetch_start = max(expiry_date - timedelta(days=90), start_date)
                
                if fetch_end > datetime.now():
                    fetch_end = datetime.now()
                
                if fetch_start >= fetch_end:
                    continue
                
                from_date = fetch_start.strftime("%d-%m-%Y")
                to_date = fetch_end.strftime("%d-%m-%Y")
                
                # Get strikes
                test_date = fetch_end.strftime("%d-%m-%Y")
                url = (f"https://www.nseindia.com/api/historicalOR/foCPV?"
                       f"from={test_date}&to={test_date}&instrumentType=OPTSTK&"
                       f"symbol={symbol}&year={year}&expiryDate={expiry_str}&optionType=CE")
                
                data = await self.fetch_url(url)
                
                if not data or 'data' not in data or not data['data']:
                    continue
                
                # Get ATM strikes
                strikes = list(set(item['FH_STRIKE_PRICE'] for item in data['data']))
                underlying_value = data['data'][0].get('FH_UNDERLYING_VALUE', 0)
                
                if underlying_value > 0:
                    atm_strikes = [s for s in strikes 
                                  if 0.9 * underlying_value <= s <= 1.1 * underlying_value]
                else:
                    atm_strikes = strikes
                
                atm_strikes = sorted(atm_strikes)[:10]
                
                # Fetch data
                expiry_records = 0
                for strike in atm_strikes:
                    for option_type in ['CE', 'PE']:
                        url = (f"https://www.nseindia.com/api/historicalOR/foCPV?"
                               f"from={from_date}&to={to_date}&instrumentType=OPTSTK&"
                               f"symbol={symbol}&year={year}&expiryDate={expiry_str}&"
                               f"optionType={option_type}&strikePrice={strike}")
                        
                        data = await self.fetch_url(url)
                        
                        if data and 'data' in data and data['data']:
                            self.save_option_data(data['data'], symbol)
                            expiry_records += len(data['data'])
                        
                        await asyncio.sleep(0.5)
                
                total_records += expiry_records
                await asyncio.sleep(2)
            
            self.log_fetch('historical', symbol, 'SUCCESS', total_records)
            logger.info(f"Completed {symbol}: {total_records} total records")
            return total_records
            
        except Exception as e:
            logger.error(f"Error fetching {symbol}: {str(e)}")
            self.log_fetch('historical', symbol, 'ERROR', 0, str(e))
            return 0
    
    async def fetch_daily_data(self, symbol: str) -> int:
        """Fetch today's data for a symbol"""
        try:
            logger.info(f"Daily fetch for {symbol}")
            
            # Fetch last 2 days to ensure we don't miss anything
            records = await self.fetch_stock_historical(symbol, days=2)
            
            self.log_fetch('daily', symbol, 'SUCCESS', records)
            return records
            
        except Exception as e:
            logger.error(f"Error in daily fetch for {symbol}: {str(e)}")
            self.log_fetch('daily', symbol, 'ERROR', 0, str(e))
            return 0
    
    def save_option_data(self, data_list: List[Dict], symbol: str):
        """Save option data to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for item in data_list:
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO option_data (
                        symbol, expiry_date, strike_price, option_type, timestamp,
                        open, high, low, close, ltp, prev_close, settle_price,
                        volume, turnover, open_interest, change_in_oi,
                        market_lot, underlying_value
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    symbol,
                    item.get('FH_EXPIRY_DT'),
                    item.get('FH_STRIKE_PRICE'),
                    item.get('FH_OPTION_TYPE'),
                    item.get('FH_TIMESTAMP'),
                    item.get('FH_OPENING_PRICE'),
                    item.get('FH_TRADE_HIGH_PRICE'),
                    item.get('FH_TRADE_LOW_PRICE'),
                    item.get('FH_CLOSING_PRICE'),
                    item.get('FH_LAST_TRADED_PRICE'),
                    item.get('FH_PREV_CLS'),
                    item.get('FH_SETTLE_PRICE'),
                    item.get('FH_TOT_TRADED_QTY'),
                    item.get('FH_TOT_TRADED_VAL'),
                    item.get('FH_OPEN_INT'),
                    item.get('FH_CHANGE_IN_OI'),
                    item.get('FH_MARKET_LOT'),
                    item.get('FH_UNDERLYING_VALUE')
                ))
            except sqlite3.IntegrityError:
                pass
        
        conn.commit()
        conn.close()
    
    def log_fetch(self, run_type: str, symbol: str, status: str, records: int, error: str = None):
        """Log fetch attempt"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO fetch_log (run_type, symbol, status, records_fetched, error_message)
            VALUES (?, ?, ?, ?, ?)
        ''', (run_type, symbol, status, records, error))
        
        conn.commit()
        conn.close()
    
    def get_stats(self) -> Dict:
        """Get database statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        stats = {}
        
        cursor.execute('SELECT COUNT(*) FROM option_data')
        stats['total_records'] = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(DISTINCT symbol) FROM option_data')
        stats['unique_symbols'] = cursor.fetchone()[0]
        
        cursor.execute('SELECT MIN(timestamp), MAX(timestamp) FROM option_data')
        result = cursor.fetchone()
        if result[0]:
            stats['date_range'] = f"{result[0]} to {result[1]}"
        else:
            stats['date_range'] = "No data"
        
        conn.close()
        return stats

# Standalone functions for different run modes
async def run_historical_fetch(stocks: List[str] = None, days: int = 180):
    """Run historical data fetch"""
    fetcher = NSEOptionsFetcher()
    
    if stocks is None:
        # Use config or environment variable
        stocks_env = os.environ.get('FETCH_STOCKS', '')
        if stocks_env:
            stocks = stocks_env.split(',')
        else:
            stocks = fetcher.nifty50_stocks[:5]  # Default to first 5
    
    days = int(os.environ.get('FETCH_DAYS', days))
    
    async with fetcher:
        results = []
        logger.info(f"Starting historical fetch for {len(stocks)} stocks, {days} days")
        
        for i, symbol in enumerate(stocks):
            logger.info(f"[{i+1}/{len(stocks)}] Processing {symbol}")
            records = await fetcher.fetch_stock_historical(symbol, days=days)
            results.append({'symbol': symbol, 'records': records})
            
            if i < len(stocks) - 1:
                await asyncio.sleep(5)
        
        stats = fetcher.get_stats()
        logger.info(f"\nFetch complete! Total records: {stats['total_records']}")
        logger.info(f"Date range: {stats['date_range']}")
        
        return results

async def run_daily_fetch():
    """Run daily update fetch"""
    fetcher = NSEOptionsFetcher()
    
    async with fetcher:
        # Get stocks that need updating
        stocks = fetcher.nifty50_stocks
        
        results = []
        logger.info(f"Starting daily fetch for {len(stocks)} stocks")
        
        for i, symbol in enumerate(stocks):
            logger.info(f"[{i+1}/{len(stocks)}] Daily update for {symbol}")
            records = await fetcher.fetch_daily_data(symbol)
            results.append({'symbol': symbol, 'records': records})
            
            if i < len(stocks) - 1:
                await asyncio.sleep(3)
        
        stats = fetcher.get_stats()
        logger.info(f"\nDaily fetch complete! Total records in DB: {stats['total_records']}")
        
        return results

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "daily":
        # Run daily fetch
        asyncio.run(run_daily_fetch())
    else:
        # Run historical fetch
        asyncio.run(run_historical_fetch())
