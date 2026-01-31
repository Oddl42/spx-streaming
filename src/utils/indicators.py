#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 31 16:56:09 2026

@author: twi-dev
"""
"""
Technische Indikatoren für Stock Analysis
Verwendet pandas-ta Library
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional


class TechnicalIndicators:
    """
    Berechnet technische Indikatoren für Stock-Daten
    """
    
    @staticmethod
    def add_moving_averages(df: pd.DataFrame, periods: List[int] = [20, 50, 200]) -> pd.DataFrame:
        """
        Fügt Simple Moving Averages hinzu
        
        Args:
            df: DataFrame mit 'close' Spalte
            periods: Liste von Perioden (z.B. [20, 50, 200])
        """
        df = df.copy()
        
        for period in periods:
            df[f'SMA_{period}'] = df['close'].rolling(window=period).mean()
        
        return df
    
    @staticmethod
    def add_exponential_moving_averages(df: pd.DataFrame, periods: List[int] = [12, 26]) -> pd.DataFrame:
        """Fügt Exponential Moving Averages hinzu"""
        df = df.copy()
        
        for period in periods:
            df[f'EMA_{period}'] = df['close'].ewm(span=period, adjust=False).mean()
        
        return df
    
    @staticmethod
    def add_bollinger_bands(df: pd.DataFrame, period: int = 20, std_dev: float = 2.0) -> pd.DataFrame:
        """
        Fügt Bollinger Bands hinzu
        
        Args:
            df: DataFrame mit 'close'
            period: Periode für SMA (default: 20)
            std_dev: Standard-Abweichungen (default: 2.0)
        """
        df = df.copy()
        
        # Middle Band (SMA)
        df['BB_Middle'] = df['close'].rolling(window=period).mean()
        
        # Standard Deviation
        rolling_std = df['close'].rolling(window=period).std()
        
        # Upper & Lower Bands
        df['BB_Upper'] = df['BB_Middle'] + (rolling_std * std_dev)
        df['BB_Lower'] = df['BB_Middle'] - (rolling_std * std_dev)
        
        return df
    
    @staticmethod
    def add_rsi(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """
        Fügt Relative Strength Index (RSI) hinzu
        
        Args:
            df: DataFrame mit 'close'
            period: Periode (default: 14)
        """
        df = df.copy()
        
        # Berechne Änderungen
        delta = df['close'].diff()
        
        # Gains und Losses
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        # RS und RSI
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))
        
        return df
    
    @staticmethod
    def add_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
        """
        Fügt MACD (Moving Average Convergence Divergence) hinzu
        
        Args:
            df: DataFrame mit 'close'
            fast: Fast EMA period (default: 12)
            slow: Slow EMA period (default: 26)
            signal: Signal line period (default: 9)
        """
        df = df.copy()
        
        # EMAs
        ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
        ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
        
        # MACD Line
        df['MACD'] = ema_fast - ema_slow
        
        # Signal Line
        df['MACD_Signal'] = df['MACD'].ewm(span=signal, adjust=False).mean()
        
        # Histogram
        df['MACD_Hist'] = df['MACD'] - df['MACD_Signal']
        
        return df
    
    @staticmethod
    def add_atr(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """
        Fügt Average True Range (ATR) hinzu
        
        Args:
            df: DataFrame mit 'high', 'low', 'close'
            period: Periode (default: 14)
        """
        df = df.copy()
        
        # True Range
        df['TR'] = np.maximum(
            df['high'] - df['low'],
            np.maximum(
                abs(df['high'] - df['close'].shift()),
                abs(df['low'] - df['close'].shift())
            )
        )
        
        # ATR
        df['ATR'] = df['TR'].rolling(window=period).mean()
        
        return df
    
    @staticmethod
    def add_all_indicators(
        df: pd.DataFrame,
        sma_periods: List[int] = [20, 50, 200],
        ema_periods: List[int] = [12, 26],
        bb_period: int = 20,
        rsi_period: int = 14,
        include_macd: bool = True,
        include_atr: bool = True
    ) -> pd.DataFrame:
        """
        Fügt alle verfügbaren Indikatoren hinzu
        
        Args:
            df: DataFrame mit OHLCV Daten
            sma_periods: SMA Perioden
            ema_periods: EMA Perioden
            bb_period: Bollinger Bands Periode
            rsi_period: RSI Periode
            include_macd: MACD hinzufügen
            include_atr: ATR hinzufügen
        """
        df = df.copy()
        
        # Moving Averages
        df = TechnicalIndicators.add_moving_averages(df, sma_periods)
        df = TechnicalIndicators.add_exponential_moving_averages(df, ema_periods)
        
        # Bollinger Bands
        df = TechnicalIndicators.add_bollinger_bands(df, bb_period)
        
        # RSI
        df = TechnicalIndicators.add_rsi(df, rsi_period)
        
        # MACD
        if include_macd:
            df = TechnicalIndicators.add_macd(df)
        
        # ATR
        if include_atr:
            df = TechnicalIndicators.add_atr(df)
        
        return df


# Test
if __name__ == "__main__":
    print("=== Technical Indicators Test ===\n")
    
    # Generate sample data
    dates = pd.date_range('2024-01-01', periods=100, freq='D')
    np.random.seed(42)
    
    df = pd.DataFrame({
        'timestamp': dates,
        'open': 100 + np.random.randn(100).cumsum(),
        'high': 102 + np.random.randn(100).cumsum(),
        'low': 98 + np.random.randn(100).cumsum(),
        'close': 100 + np.random.randn(100).cumsum(),
        'volume': np.random.randint(1000000, 5000000, 100)
    })
    
    # Add all indicators
    df = TechnicalIndicators.add_all_indicators(df)
    
    print("Columns after adding indicators:")
    print(df.columns.tolist())
    
    print("\nLast 5 rows:")
    print(df[['timestamp', 'close', 'SMA_20', 'RSI', 'MACD']].tail())
    
    print("\n✓ Indicators calculated successfully!")
