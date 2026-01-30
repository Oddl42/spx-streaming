#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 30 21:33:41 2026

@author: twi-dev
"""

# src/streaming/stream_manager.py
"""
Stream Manager für Live-Daten
Verwaltet Rolling Windows und Data Buffers
"""

from collections import deque
from typing import Dict, List, Optional
import pandas as pd
from datetime import datetime
import threading
import pytz

from src.utils.market_hours import MarketHours


class StreamDataManager:
    """
    Verwaltet Live-Stream-Daten mit Rolling Windows
    
    Features:
    - Rolling Window für Sekunden-Daten (600 Punkte)
    - Buffer für Minuten-Daten
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
    
    def add_data_point(self, ticker: str, data: Dict):
        """
        Fügt Datenpunkt hinzu
        
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
                # Massive gibt Unix timestamp in Millisekunden
                timestamp_ms = data['timestamp']
                dt_utc = datetime.fromtimestamp(timestamp_ms / 1000, tz=pytz.UTC)
                dt_et = MarketHours.utc_to_eastern(dt_utc)
                data['timestamp_et'] = dt_et
            
            # Prüfe Market Hours (nur Regular Hours)
            if 'timestamp_et' in data:
                if not MarketHours.is_market_open(data['timestamp_et']):
                    # Ignoriere Daten außerhalb Market Hours
                    return
            
            # Füge Datenpunkt hinzu
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
        """Gibt letzten Datenpunkt für Ticker zurück"""
        with self.lock:
            if ticker not in self.data_buffers or not self.data_buffers[ticker]:
                return None
            return self.data_buffers[ticker][-1]
    
    def get_buffer_size(self, ticker: str) -> int:
        """Gibt Anzahl Datenpunkte für Ticker zurück"""
        with self.lock:
            if ticker not in self.data_buffers:
                return 0
            return len(self.data_buffers[ticker])
    
    def is_buffer_full(self, ticker: str) -> bool:
        """Prüft ob Buffer voll ist (600 Punkte)"""
        return self.get_buffer_size(ticker) >= self.max_points
    
    def clear_ticker(self, ticker: str):
        """Löscht Buffer für Ticker"""
        with self.lock:
            if ticker in self.data_buffers:
                self.data_buffers[ticker].clear()
    
    def clear_all(self):
        """Löscht alle Buffer"""
        with self.lock:
            self.data_buffers.clear()
            self.messages_per_ticker.clear()
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
                }
            }
