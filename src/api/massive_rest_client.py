#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 30 16:19:22 2026

@author: twi-dev
"""

import os
import requests
from typing import Optional, List, Dict
from datetime import datetime, timedelta
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

class MassiveRESTClient:
    """REST API Client für Massive.com"""
    
    BASE_URL = "https://api.massive.com/v2"
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv('MASSIVE_API_KEY')
        if not self.api_key:
            raise ValueError("API Key nicht gefunden! Setze MASSIVE_API_KEY in .env")
        
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {self.api_key}'
        })
    
    def test_connection(self) -> bool:
        """Testet ob API-Verbindung funktioniert"""
        try:
            # Teste mit einfachem Request
            endpoint = f"{self.BASE_URL}/meta/symbols/stocks"
            response = self.session.get(endpoint)
            response.raise_for_status()
            print("✓ API-Verbindung erfolgreich!")
            return True
        except Exception as e:
            print(f"✗ API-Verbindung fehlgeschlagen: {e}")
            return False
    
    def get_aggregates(
        self, 
        ticker: str,
        multiplier: int = 1,
        timespan: str = 'minute',
        from_date: str = None,
        to_date: str = None,
        limit: int = 600
    ) -> pd.DataFrame:
        """
        Lädt aggregierte Bars für einen Ticker
        
        Args:
            ticker: Ticker Symbol (z.B. 'AAPL')
            multiplier: Multiplikator (1 = 1 Minute)
            timespan: 'minute', 'hour', 'day'
            from_date: Start-Datum (YYYY-MM-DD)
            to_date: End-Datum (YYYY-MM-DD)
            limit: Max. Anzahl Results (max 50000)
        """
        if not to_date:
            to_date = datetime.now().strftime('%Y-%m-%d')
        
        if not from_date:
            # ✨ GEÄNDERT: Berechne from_date basierend auf timespan
            if timespan == 'minute':
                # Für Minuten-Daten: Letzte 3 Handelstage (ca. 1170 Minuten)
                from_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
            elif timespan == 'hour':
                # Für Stunden-Daten: Letzte 30 Tage
                from_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            elif timespan == 'day':
                # Für Tages-Daten: Letzte 5 Jahre
                from_date = (datetime.now() - timedelta(days=5*365)).strftime('%Y-%m-%d')
            else:
                from_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        endpoint = (f"{self.BASE_URL}/aggs/ticker/{ticker}/range/"
                   f"{multiplier}/{timespan}/{from_date}/{to_date}")
        
        params = {
            'adjusted': 'true',
            'sort': 'asc',
            'limit': limit
        }
        
        try:
            print(f"\n🔍 API Request: {endpoint}")
            print(f"   Params: {params}")
            print(f"   Zeitraum: {from_date} bis {to_date}")
            
            response = self.session.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()
            
            if 'results' not in data or not data['results']:
                print(f"⚠️  Keine Daten für {ticker} im Zeitraum {from_date} bis {to_date}")
                print(f"   API Response: {data}")
                return pd.DataFrame()
            
            df = pd.DataFrame(data['results'])
            
            # Timestamp zu Datetime konvertieren
            df['timestamp'] = pd.to_datetime(df['t'], unit='ms')
            
            # Umbenennen für Konsistenz
            df = df.rename(columns={
                'o': 'open',
                'h': 'high',
                'l': 'low',
                'c': 'close',
                'v': 'volume',
                'vw': 'vwap',
                'n': 'transactions'
            })
            
            df = df[['timestamp', 'open', 'high', 'low', 'close', 
                    'volume', 'vwap', 'transactions']]
            
            print(f"✓ {len(df)} Datenpunkte für {ticker} geladen")
            print(f"   Erster Punkt: {df['timestamp'].min()}")
            print(f"   Letzter Punkt: {df['timestamp'].max()}")
            
            return df
            
        except Exception as e:
            print(f"✗ Fehler beim Laden von {ticker}: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    
    def get_last_quote(self, ticker: str) -> Dict:
        """Lädt letztes Quote für einen Ticker"""
        endpoint = f"{self.BASE_URL}/last/quote/{ticker}"
        
        try:
            response = self.session.get(endpoint)
            response.raise_for_status()
            data = response.json()
            print(f"✓ Last Quote für {ticker}: ${data['last']['last']}")
            return data
        except Exception as e:
            print(f"✗ Fehler: {e}")
            return {}

# Test-Funktion
if __name__ == "__main__":
    print("=== Massive API Client Test ===\n")
    
    client = MassiveRESTClient()
    
    # 1. Verbindung testen
    if client.test_connection():
        print()
        
        # 2. Historische Daten abrufen
        print("2. Lade historische Daten für AAPL...")
        df = client.get_aggregates('AAPL', timespan='minute', limit=100)
        if not df.empty:
            print(df.head())
            print(f"\nDaterange: {df['timestamp'].min()} bis {df['timestamp'].max()}")
        
        print()
        
        # 3. Letztes Quote
        print("3. Lade letztes Quote für AAPL...")
        quote = client.get_last_quote('AAPL')
