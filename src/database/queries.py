#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb  1 21:26:10 2026

@author: twi-dev
"""

# src/database/queries.py
"""
Database Query Functions für historische Stock-Daten
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List
from sqlalchemy import text

from src.database.connection import db_manager
from src.database.models import DailyBar, MinuteBar, Ticker


class StockDataQueries:
    """
    Query-Funktionen für Stock-Daten aus TimescaleDB
    """
    
    @staticmethod
    def get_tickers() -> pd.DataFrame:
        """
        Lädt alle S&P 500 Ticker aus DB
        
        Returns:
            DataFrame mit Ticker-Informationen
        """
        with db_manager.get_session() as session:
            result = session.query(Ticker).all()
            
            if not result:
                return pd.DataFrame()
            
            data = [{
                'Symbol': t.symbol,
                'Security': t.security,
                'GICS Sector': t.gics_sector,
                'GICS Sub-Industry': t.gics_sub_industry,
                'Headquarters Location': t.headquarters_location,
                'Date added': t.date_added,
                'CIK': t.cik,
                'Founded': t.founded
            } for t in result]
            
            return pd.DataFrame(data)
    
    @staticmethod
    def get_daily_bars(
        symbol: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Lädt Daily Bars für einen Ticker
        
        Args:
            symbol: Ticker Symbol
            start_date: Start-Datum (optional)
            end_date: End-Datum (optional)
            limit: Max. Anzahl Bars (optional)
            
        Returns:
            DataFrame mit OHLCV Daten
        """
        with db_manager.get_session() as session:
            # Base Query
            query = """
                SELECT 
                    time as timestamp,
                    symbol,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    vwap,
                    transactions
                FROM daily_bars
                WHERE symbol = :symbol
            """
            
            params = {'symbol': symbol}
            
            # Datum-Filter
            if start_date:
                query += " AND time >= :start_date"
                params['start_date'] = start_date
            
            if end_date:
                query += " AND time <= :end_date"
                params['end_date'] = end_date
            
            # Sortierung und Limit
            query += " ORDER BY time DESC"
            
            if limit:
                query += " LIMIT :limit"
                params['limit'] = limit
            
            # Execute
            result = session.execute(text(query), params)
            
            # Zu DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            
            if df.empty:
                return df
            
            # Konvertiere Timestamp
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Sortiere aufsteigend
            df = df.sort_values('timestamp').reset_index(drop=True)
            
            return df
    
    @staticmethod
    def get_minute_bars(
        symbol: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Lädt Minute Bars für einen Ticker
        
        Args:
            symbol: Ticker Symbol
            start_date: Start-Datum (optional)
            end_date: End-Datum (optional)
            limit: Max. Anzahl Bars (optional)
            
        Returns:
            DataFrame mit OHLCV Daten
        """
        with db_manager.get_session() as session:
            # Base Query
            query = """
                SELECT 
                    time as timestamp,
                    symbol,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    vwap,
                    transactions
                FROM minute_bars
                WHERE symbol = :symbol
            """
            
            params = {'symbol': symbol}
            
            # Datum-Filter
            if start_date:
                query += " AND time >= :start_date"
                params['start_date'] = start_date
            
            if end_date:
                query += " AND time <= :end_date"
                params['end_date'] = end_date
            
            # Sortierung und Limit
            query += " ORDER BY time DESC"
            
            if limit:
                query += " LIMIT :limit"
                params['limit'] = limit
            
            # Execute
            result = session.execute(text(query), params)
            
            # Zu DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            
            if df.empty:
                return df
            
            # Konvertiere Timestamp
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Sortiere aufsteigend
            df = df.sort_values('timestamp').reset_index(drop=True)
            
            return df
    
    @staticmethod
    def get_aggregated_bars(
        symbol: str,
        timespan: str,  # 'minute', '5minute', '15minute', 'hour', 'day'
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Lädt aggregierte Bars (unterstützt verschiedene Timeframes)
        
        Args:
            symbol: Ticker Symbol
            timespan: '1minute', '5minute', '15minute', 'hour', 'day'
            start_date: Start-Datum
            end_date: End-Datum
            limit: Max. Anzahl Bars
            
        Returns:
            DataFrame mit aggregierten OHLCV Daten
        """
        # Basis-Tabelle bestimmen
        if timespan == 'day':
            return StockDataQueries.get_daily_bars(symbol, start_date, end_date, limit)
        
        # Für Minute-basierte Timeframes: Aus minute_bars aggregieren
        aggregation_map = {
            'minute': '1 minute',
            '5minute': '5 minutes',
            '15minute': '15 minutes',
            'hour': '1 hour'
        }
        
        interval = aggregation_map.get(timespan, '1 minute')
        
        with db_manager.get_session() as session:
            query = f"""
                SELECT 
                    time_bucket(:interval, time) as timestamp,
                    symbol,
                    first(open, time) as open,
                    max(high) as high,
                    min(low) as low,
                    last(close, time) as close,
                    sum(volume) as volume,
                    avg(vwap) as vwap,
                    sum(transactions) as transactions
                FROM minute_bars
                WHERE symbol = :symbol
            """
            
            params = {
                'symbol': symbol,
                'interval': interval
            }
            
            if start_date:
                query += " AND time >= :start_date"
                params['start_date'] = start_date
            
            if end_date:
                query += " AND time <= :end_date"
                params['end_date'] = end_date
            
            query += """
                GROUP BY timestamp, symbol
                ORDER BY timestamp DESC
            """
            
            if limit:
                query += " LIMIT :limit"
                params['limit'] = limit
            
            # Execute
            result = session.execute(text(query), params)
            
            # Zu DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            
            if df.empty:
                return df
            
            # Konvertiere Timestamp
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Sortiere aufsteigend
            df = df.sort_values('timestamp').reset_index(drop=True)
            
            return df
    
    @staticmethod
    def get_data_statistics(symbol: str) -> dict:
        """
        Gibt Statistiken über verfügbare Daten zurück
        
        Returns:
            dict mit Statistiken
        """
        with db_manager.get_session() as session:
            # Daily Stats
            daily_stats = session.execute(text("""
                SELECT 
                    COUNT(*) as count,
                    MIN(time) as min_date,
                    MAX(time) as max_date
                FROM daily_bars
                WHERE symbol = :symbol
            """), {'symbol': symbol}).fetchone()
            
            # Minute Stats
            minute_stats = session.execute(text("""
                SELECT 
                    COUNT(*) as count,
                    MIN(time) as min_date,
                    MAX(time) as max_date
                FROM minute_bars
                WHERE symbol = :symbol
            """), {'symbol': symbol}).fetchone()
            
            return {
                'daily': {
                    'count': daily_stats[0] if daily_stats else 0,
                    'min_date': daily_stats[1] if daily_stats and daily_stats[1] else None,
                    'max_date': daily_stats[2] if daily_stats and daily_stats[2] else None
                },
                'minute': {
                    'count': minute_stats[0] if minute_stats else 0,
                    'min_date': minute_stats[1] if minute_stats and minute_stats[1] else None,
                    'max_date': minute_stats[2] if minute_stats and minute_stats[2] else None
                }
            }
    
    @staticmethod
    def get_latest_price(symbol: str) -> Optional[float]:
        """
        Gibt den letzten verfügbaren Preis zurück
        
        Returns:
            float: Letzter Close-Preis oder None
        """
        with db_manager.get_session() as session:
            result = session.execute(text("""
                SELECT close
                FROM daily_bars
                WHERE symbol = :symbol
                ORDER BY time DESC
                LIMIT 1
            """), {'symbol': symbol}).fetchone()
            
            return result[0] if result else None


# Singleton Instance
stock_queries = StockDataQueries()


# Test
if __name__ == "__main__":
    print("=== Database Queries Test ===\n")
    
    # Test 1: Ticker laden
    print("1. Lade Ticker...")
    tickers_df = stock_queries.get_tickers()
    print(f"   ✓ {len(tickers_df)} Ticker geladen")
    print(f"   Erste 3: {tickers_df['Symbol'].head(3).tolist()}")
    
    # Test 2: Daily Bars
    print("\n2. Lade Daily Bars für AAPL...")
    daily_df = stock_queries.get_daily_bars('AAPL', limit=10)
    if not daily_df.empty:
        print(f"   ✓ {len(daily_df)} Daily Bars geladen")
        print(f"   Zeitraum: {daily_df['timestamp'].min()} bis {daily_df['timestamp'].max()}")
    
    # Test 3: Minute Bars
    print("\n3. Lade Minute Bars für AAPL...")
    minute_df = stock_queries.get_minute_bars('AAPL', limit=100)
    if not minute_df.empty:
        print(f"   ✓ {len(minute_df)} Minute Bars geladen")
        print(f"   Zeitraum: {minute_df['timestamp'].min()} bis {minute_df['timestamp'].max()}")
    
    # Test 4: Aggregierte Daten (5min)
    print("\n4. Lade 5-Minuten Bars für AAPL...")
    agg_df = stock_queries.get_aggregated_bars('AAPL', '5minute', limit=50)
    if not agg_df.empty:
        print(f"   ✓ {len(agg_df)} 5-Min Bars geladen")
    
    # Test 5: Statistiken
    print("\n5. Daten-Statistiken für AAPL...")
    stats = stock_queries.get_data_statistics('AAPL')
    print(f"   Daily Bars: {stats['daily']['count']:,}")
    print(f"   Minute Bars: {stats['minute']['count']:,}")
    
    print("\n✅ Alle Tests erfolgreich!")
