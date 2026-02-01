#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 31 17:19:32 2026

@author: twi-dev
"""

# scripts/download_historical_data.py
"""
Download S&P 500 Historical Data Script
- Daily Bars: seit 1.1.2020
- Minute Bars: seit 1.1.2023
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

import pandas as pd
from datetime import datetime, date
import time
from typing import List, Dict
from sqlalchemy import text

from src.utils.helpers import SP500TickerLoader
from src.api.massive_rest_client import MassiveRESTClient
from src.database.connection import db_manager
from src.database.models import Ticker, DailyBar, MinuteBar, DownloadStatus


class HistoricalDataDownloader:
    """
    Lädt historische Daten für alle S&P 500 Ticker
    """
    
    # Zeiträume
    DAILY_START_DATE = "2020-01-01"
    MINUTE_START_DATE = "2023-01-01"
    
    def __init__(self):
        self.ticker_loader = SP500TickerLoader()
        self.api_client = MassiveRESTClient()
        self.db = db_manager
        
        # Rate Limiting
        self.requests_per_minute = 30  # Passen Sie an Ihren API-Plan an
        self.delay_between_requests = 60.0 / self.requests_per_minute
        
        # Statistics
        self.stats = {
            'tickers_total': 0,
            'tickers_completed': 0,
            'tickers_failed': 0,
            'daily_bars_total': 0,
            'minute_bars_total': 0,
            'errors': []
        }
    
    def load_tickers_to_db(self):
        """Lädt S&P 500 Ticker in die Datenbank"""
        print("\n📋 Lade S&P 500 Ticker in Datenbank...")
        
        df = self.ticker_loader.load_tickers()
        
        with self.db.get_session() as session:
            for _, row in df.iterrows():
                ticker = Ticker(
                    symbol=row['Symbol'],
                    security=row['Security'],
                    gics_sector=row.get('GICS Sector'),
                    gics_sub_industry=row.get('GICS Sub-Industry'),
                    headquarters_location=row.get('Headquarters Location'),
                    date_added=pd.to_datetime(row.get('Date added')).date() if pd.notna(row.get('Date added')) else None,
                    cik=int(row['CIK']) if pd.notna(row.get('CIK')) else None,
                    founded=str(row.get('Founded')) if pd.notna(row.get('Founded')) else None
                )
                
                session.merge(ticker)  # Insert or Update
            
            session.commit()
            print(f"✓ {len(df)} Ticker in DB gespeichert")
        
        return df['Symbol'].tolist()
    
    def download_daily_data(self, symbol: str) -> int:
        """
        Lädt Daily Bars für einen Ticker
        
        Returns:
            int: Anzahl gespeicherter Bars
        """
        print(f"\n📊 {symbol}: Lade Daily Bars (seit {self.DAILY_START_DATE})...")
        
        try:
            # API Call
            df = self.api_client.get_aggregates(
                ticker=symbol,
                timespan='day',
                from_date=self.DAILY_START_DATE,
                to_date=datetime.now().strftime('%Y-%m-%d'),
                limit=50000
            )
            
            if df.empty:
                print(f"   ⚠️ Keine Daten für {symbol}")
                return 0
            
            # In DB speichern
            with self.db.get_session() as session:
                bars_inserted = 0
                
                for _, row in df.iterrows():
                    bar = DailyBar(
                        time=row['timestamp'],
                        symbol=symbol,
                        open=row['open'],
                        high=row['high'],
                        low=row['low'],
                        close=row['close'],
                        volume=row['volume'],
                        vwap=row.get('vwap'),
                        transactions=row.get('transactions')
                    )
                    
                    session.merge(bar)
                    bars_inserted += 1
                
                session.commit()
                print(f"   ✓ {bars_inserted} Daily Bars gespeichert")
                
                return bars_inserted
        
        except Exception as e:
            print(f"   ✗ Fehler: {e}")
            return 0
    
    def download_minute_data(self, symbol: str) -> int:
        """
        Lädt Minute Bars für einen Ticker (seit 2023)
        
        Returns:
            int: Anzahl gespeicherter Bars
        """
        print(f"\n📊 {symbol}: Lade Minute Bars (seit {self.MINUTE_START_DATE})...")
        
        try:
            # Berechne Zeitraum in Chunks (max 50.000 Bars pro Request)
            # 1 Jahr ≈ 252 Handelstage × 390 Minuten = 98.280 Bars
            # Also: Lade in Jahres-Chunks
            
            start_date = datetime.strptime(self.MINUTE_START_DATE, '%Y-%m-%d')
            end_date = datetime.now()
            
            total_bars = 0
            current_start = start_date
            
            while current_start < end_date:
                # 1 Jahr Chunk
                current_end = min(
                    current_start.replace(year=current_start.year + 1),
                    end_date
                )
                
                print(f"   Chunk: {current_start.date()} bis {current_end.date()}")
                
                # API Call
                df = self.api_client.get_aggregates(
                    ticker=symbol,
                    timespan='minute',
                    from_date=current_start.strftime('%Y-%m-%d'),
                    to_date=current_end.strftime('%Y-%m-%d'),
                    limit=50000
                )
                
                if not df.empty:
                    # In DB speichern (Batch Insert für Performance)
                    with self.db.get_session() as session:
                        bars = []
                        
                        for _, row in df.iterrows():
                            bar = MinuteBar(
                                time=row['timestamp'],
                                symbol=symbol,
                                open=row['open'],
                                high=row['high'],
                                low=row['low'],
                                close=row['close'],
                                volume=row['volume'],
                                vwap=row.get('vwap'),
                                transactions=row.get('transactions')
                            )
                            bars.append(bar)
                        
                        # Batch Insert
                        session.bulk_save_objects(bars)
                        session.commit()
                        
                        total_bars += len(bars)
                        print(f"   ✓ {len(bars)} Bars gespeichert")
                
                # Nächster Chunk
                current_start = current_end
                
                # Rate Limiting
                time.sleep(self.delay_between_requests)
            
            print(f"   ✓ Gesamt: {total_bars} Minute Bars gespeichert")
            return total_bars
        
        except Exception as e:
            print(f"   ✗ Fehler: {e}")
            return 0
    
    def update_download_status(self, symbol: str, timespan: str, 
                               status: str, bars: int = 0, error: str = None):
        """Aktualisiert Download Status in DB"""
        with self.db.get_session() as session:
            # Suche existierenden Eintrag
            existing = session.query(DownloadStatus).filter_by(
                symbol=symbol,
                timespan=timespan
            ).first()
            
            if existing:
                existing.status = status
                existing.bars_downloaded = bars
                existing.error_message = error
                if status == 'running':
                    existing.started_at = datetime.now()
                elif status in ['completed', 'failed']:
                    existing.completed_at = datetime.now()
            else:
                # Neuer Eintrag
                ds = DownloadStatus(
                    symbol=symbol,
                    timespan=timespan,
                    start_date=datetime.strptime(
                        self.DAILY_START_DATE if timespan == 'day' else self.MINUTE_START_DATE,
                        '%Y-%m-%d'
                    ).date(),
                    end_date=date.today(),
                    status=status,
                    bars_downloaded=bars,
                    error_message=error,
                    started_at=datetime.now() if status == 'running' else None,
                    completed_at=datetime.now() if status in ['completed', 'failed'] else None
                )
                session.add(ds)
            
            session.commit()
    
    def download_all(self, tickers: List[str] = None, skip_existing: bool = True):
        """
        Lädt Daten für alle Ticker
        
        Args:
            tickers: Liste von Tickern (None = alle S&P 500)
            skip_existing: Überspringe Ticker mit existierenden Daten
        """
        print("=" * 60)
        print("S&P 500 HISTORICAL DATA DOWNLOAD")
        print("=" * 60)
        
        # 1. Lade Ticker
        if tickers is None:
            tickers = self.load_tickers_to_db()
        
        self.stats['tickers_total'] = len(tickers)
        
        # 2. Download Loop
        for i, symbol in enumerate(tickers, 1):
            print(f"\n{'='*60}")
            print(f"[{i}/{len(tickers)}] {symbol}")
            print(f"{'='*60}")
            
            try:
                # Check if already downloaded
                if skip_existing:
                    with self.db.get_session() as session:
                        daily_exists = session.query(DailyBar).filter_by(symbol=symbol).limit(1).count() > 0
                        minute_exists = session.query(MinuteBar).filter_by(symbol=symbol).limit(1).count() > 0
                        
                        if daily_exists and minute_exists:
                            print(f"✓ Daten bereits vorhanden, überspringe...")
                            self.stats['tickers_completed'] += 1
                            continue
                
                # Download Daily
                self.update_download_status(symbol, 'day', 'running')
                daily_bars = self.download_daily_data(symbol)
                self.stats['daily_bars_total'] += daily_bars
                
                if daily_bars > 0:
                    self.update_download_status(symbol, 'day', 'completed', daily_bars)
                else:
                    self.update_download_status(symbol, 'day', 'failed', 0, 'No data')
                
                # Rate Limiting
                time.sleep(self.delay_between_requests)
                
                # Download Minute
                self.update_download_status(symbol, 'minute', 'running')
                minute_bars = self.download_minute_data(symbol)
                self.stats['minute_bars_total'] += minute_bars
                
                if minute_bars > 0:
                    self.update_download_status(symbol, 'minute', 'completed', minute_bars)
                else:
                    self.update_download_status(symbol, 'minute', 'failed', 0, 'No data')
                
                self.stats['tickers_completed'] += 1
                
            except Exception as e:
                print(f"\n✗ Fehler bei {symbol}: {e}")
                self.stats['tickers_failed'] += 1
                self.stats['errors'].append({'symbol': symbol, 'error': str(e)})
                
                # Status als failed
                self.update_download_status(symbol, 'day', 'failed', 0, str(e))
                self.update_download_status(symbol, 'minute', 'failed', 0, str(e))
            
            # Progress
            progress = (i / len(tickers)) * 100
            print(f"\n📊 Fortschritt: {progress:.1f}% ({i}/{len(tickers)})")
        
        # 3. Final Report
        self.print_summary()
    
    def print_summary(self):
        """Druckt Zusammenfassung"""
        print("\n" + "=" * 60)
        print("DOWNLOAD ABGESCHLOSSEN")
        print("=" * 60)
        print(f"\n📊 Statistiken:")
        print(f"   Ticker gesamt: {self.stats['tickers_total']}")
        print(f"   ✓ Erfolgreich: {self.stats['tickers_completed']}")
        print(f"   ✗ Fehlgeschlagen: {self.stats['tickers_failed']}")
        print(f"   📈 Daily Bars: {self.stats['daily_bars_total']:,}")
        print(f"   📊 Minute Bars: {self.stats['minute_bars_total']:,}")
        
        if self.stats['errors']:
            print(f"\n❌ Fehler ({len(self.stats['errors'])}):")
            for err in self.stats['errors'][:10]:
                print(f"   • {err['symbol']}: {err['error']}")
        
        # DB Stats
        print(f"\n🗄️ Datenbank Statistiken:")
        db_stats = self.db.get_stats()
        for key, value in db_stats.items():
            print(f"   {key}: {value}")


# ============================================
# Main Execution
# ============================================

if __name__ == "__main__":
    print("Teste Datenbank-Verbindung...")
    if not db_manager.test_connection():
        print("❌ Datenbank nicht erreichbar!")
        print("Starte Docker: docker-compose up -d")
        sys.exit(1)
    
    downloader = HistoricalDataDownloader()
    
    # TEST MODE: Nur 3 Ticker
    test_mode = input("\n🧪 Test-Modus (nur 3 Ticker)? (j/n): ").lower() == 'j'
    
    if test_mode:
        print("\n🧪 TEST MODUS: Lade nur AAPL, MSFT, GOOGL")
        downloader.download_all(['AAPL', 'MSFT', 'GOOGL'], skip_existing=False)
    else:
        print("\n🚀 FULL MODE: Lade alle S&P 500 Ticker")
        confirm = input("Dies kann mehrere Stunden dauern. Fortfahren? (j/n): ")
        
        if confirm.lower() == 'j':
            downloader.download_all(skip_existing=True)
        else:
            print("Abgebrochen.")
