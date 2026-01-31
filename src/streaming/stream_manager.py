# src/streaming/stream_manager.py
"""
Stream Manager für Live-Daten
MIT Pre-Loading von historischen Daten
"""

from collections import deque
from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime, timedelta
import threading
import pytz

from src.utils.market_hours import MarketHours


class StreamDataManager:
    """
    Verwaltet Live-Stream-Daten mit Rolling Windows
    
    Features:
    - Rolling Window für Sekunden-Daten (600 Punkte)
    - Buffer für Minuten-Daten
    - Pre-Loading von historischen Daten
    - Thread-safe Operations
    - Automatische Market Hours Filterung
    """
    
    def __init__(self, max_points: int = 600):
        """
        Args:
            max_points: Maximale Anzahl Datenpunkte im Rolling Window
        """
        self.max_points = max_points
        
        # Data Buffers pro Ticker
        self.data_buffers: Dict[str, deque] = {}
        
        # Lock für thread-safe operations
        self.lock = threading.Lock()
        
        # Statistics
        self.total_messages = 0
        self.messages_per_ticker: Dict[str, int] = {}
        
        # Pre-load Status
        self.preloaded_tickers: List[str] = []
    
    def preload_historical_data(self, ticker: str, df: pd.DataFrame):
        """
        Lädt historische Daten in den Buffer
        
        Args:
            ticker: Ticker Symbol
            df: DataFrame mit historischen OHLCV Daten
        """
        with self.lock:
            if ticker not in self.data_buffers:
                self.data_buffers[ticker] = deque(maxlen=self.max_points)
                self.messages_per_ticker[ticker] = 0
            
            # Konvertiere DataFrame zu Dict-Liste
            for _, row in df.iterrows():
                data_point = {
                    'symbol': ticker,
                    'timestamp': row.get('timestamp'),
                    'timestamp_et': row.get('timestamp'),
                    'open': row.get('open'),
                    'high': row.get('high'),
                    'low': row.get('low'),
                    'close': row.get('close'),
                    'volume': row.get('volume'),
                    'vwap': row.get('vwap'),
                    'transactions': row.get('transactions', 0),
                    'is_preloaded': True  # Markiere als pre-loaded
                }
                self.data_buffers[ticker].append(data_point)
            
            self.preloaded_tickers.append(ticker)
            print(f"✓ {len(df)} historische Datenpunkte für {ticker} geladen")
    
    def add_data_point(self, ticker: str, data: Dict):
        """
        Fügt Live-Datenpunkt hinzu
        
        Args:
            ticker: Ticker Symbol
            data: Dictionary mit OHLCV Daten
        """
        with self.lock:
            # Buffer erstellen falls nicht existiert
            if ticker not in self.data_buffers:
                self.data_buffers[ticker] = deque(maxlen=self.max_points)
                self.messages_per_ticker[ticker] = 0
            
            # Timestamp zu Eastern Time konvertieren
            if 'timestamp' in data and data['timestamp']:
                timestamp_ms = data['timestamp']
                dt_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=pytz.UTC)
                dt_et = MarketHours.utc_to_eastern(dt_utc)
                data['timestamp_et'] = dt_et
            
            # Prüfe Market Hours (nur Regular Hours)
            if 'timestamp_et' in data:
                if not MarketHours.is_market_open(data['timestamp_et']):
                    return
            
            # Markiere als Live-Daten
            data['is_preloaded'] = False
            
            # Füge hinzu
            self.data_buffers[ticker].append(data)
            self.messages_per_ticker[ticker] += 1
            self.total_messages += 1
    
    def get_dataframe(self, ticker: str) -> pd.DataFrame:
        """
        Gibt DataFrame für Ticker zurück
        
        Args:
            ticker: Ticker Symbol
            
        Returns:
            DataFrame mit allen Datenpunkten
        """
        with self.lock:
            if ticker not in self.data_buffers:
                return pd.DataFrame()
            
            data_list = list(self.data_buffers[ticker])
            
            if not data_list:
                return pd.DataFrame()
            
            df = pd.DataFrame(data_list)
            
            # Konvertiere Timestamp
            if 'timestamp_et' in df.columns:
                df['timestamp'] = df['timestamp_et']
            
            # Sortiere nach Zeit
            if 'timestamp' in df.columns:
                df = df.sort_values('timestamp')
            
            return df
    
    def get_latest_point(self, ticker: str) -> Optional[Dict]:
        """Gibt letzten Datenpunkt zurück"""
        with self.lock:
            if ticker not in self.data_buffers or not self.data_buffers[ticker]:
                return None
            return self.data_buffers[ticker][-1]
    
    def get_buffer_size(self, ticker: str) -> int:
        """Gibt Anzahl Datenpunkte zurück"""
        with self.lock:
            if ticker not in self.data_buffers:
                return 0
            return len(self.data_buffers[ticker])
    
    def is_buffer_full(self, ticker: str) -> bool:
        """Prüft ob Buffer voll ist"""
        return self.get_buffer_size(ticker) >= self.max_points
    
    def get_buffer_status(self, ticker: str) -> Dict:
        """Gibt detaillierten Buffer-Status zurück"""
        with self.lock:
            if ticker not in self.data_buffers:
                return {
                    'size': 0,
                    'max': self.max_points,
                    'percentage': 0,
                    'is_full': False,
                    'preloaded': False,
                    'live_count': 0
                }
            
            buffer = self.data_buffers[ticker]
            data_list = list(buffer)
            
            preloaded_count = sum(1 for d in data_list if d.get('is_preloaded', False))
            live_count = len(data_list) - preloaded_count
            
            return {
                'size': len(buffer),
                'max': self.max_points,
                'percentage': int((len(buffer) / self.max_points) * 100),
                'is_full': len(buffer) >= self.max_points,
                'preloaded': ticker in self.preloaded_tickers,
                'preloaded_count': preloaded_count,
                'live_count': live_count
            }
    
    def clear_ticker(self, ticker: str):
        """Löscht Buffer für Ticker"""
        with self.lock:
            if ticker in self.data_buffers:
                self.data_buffers[ticker].clear()
            if ticker in self.preloaded_tickers:
                self.preloaded_tickers.remove(ticker)
    
    def clear_all(self):
        """Löscht alle Buffer"""
        with self.lock:
            self.data_buffers.clear()
            self.messages_per_ticker.clear()
            self.preloaded_tickers.clear()
            self.total_messages = 0
    
    def get_statistics(self) -> Dict:
        """Gibt Statistiken zurück"""
        with self.lock:
            return {
                'total_messages': self.total_messages,
                'active_tickers': len(self.data_buffers),
                'messages_per_ticker': self.messages_per_ticker.copy(),
                'buffer_sizes': {
                    ticker: len(buffer) 
                    for ticker, buffer in self.data_buffers.items()
                },
                'preloaded_tickers': self.preloaded_tickers.copy()
            }
