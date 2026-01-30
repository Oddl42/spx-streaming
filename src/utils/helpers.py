# -*- coding: utf-8 -*-

# src/utils/helpers.py
import os
import json
from pathlib import Path
from typing import List, Dict
import pandas as pd

class SP500TickerLoader:
    """Lädt und verwaltet S&P 500 Ticker aus JSON"""
    
    def __init__(self):
        self.root_path = Path(__file__).resolve().parent.parent.parent 
        self.json_path = self.root_path/"data"/"flat-ui__data-Fri_Jan_23_2026.json"
        self.tickers_data = None
        
    def load_tickers(self) -> pd.DataFrame:
        """Lädt alle S&P 500 Ticker aus JSON"""
        with open(self.json_path, 'r', encoding='utf-8') as f:
            self.tickers_data = json.load(f)
        
        df = pd.DataFrame(self.tickers_data)
        print(f"✓ {len(df)} S&P 500 Ticker geladen")
        return df
    
    def get_ticker_symbols(self) -> List[str]:
        """Gibt Liste aller Ticker-Symbole zurück"""
        if self.tickers_data is None:
            self.load_tickers()
        return [ticker['Symbol'] for ticker in self.tickers_data]
    
    def get_ticker_info(self, symbol: str) -> Dict:
        """Gibt detaillierte Info zu einem Ticker"""
        if self.tickers_data is None:
            self.load_tickers()
        
        for ticker in self.tickers_data:
            if ticker['Symbol'] == symbol:
                return ticker
        return None
    
    def search_tickers(self, query: str) -> List[Dict]:
        """Sucht Ticker nach Symbol oder Name"""
        if self.tickers_data is None:
            self.load_tickers()
        
        query = query.upper()
        results = []
        for ticker in self.tickers_data:
            if (query in ticker['Symbol'].upper() or 
                query in ticker['Security'].upper()):
                results.append(ticker)
        return results

# Test-Funktion
if __name__ == "__main__":
    loader = SP500TickerLoader()
    df = loader.load_tickers()
    print("\nErste 5 Ticker:")
    print(df[['Symbol', 'Security', 'GICS Sector']].head())
    
    print("\nSuche nach 'AAPL':")
    print(loader.get_ticker_info('AAPL'))
