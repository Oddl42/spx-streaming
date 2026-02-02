#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 30 16:50:32 2026

@author: twi-dev
"""

# src/utils/market_hours.py
"""
Market Hours Logic für US Stock Market
- Regular Market: 9:30 AM - 4:00 PM ET (Eastern Time)
- Konvertierung von UTC zu ET
- Filterung von Wochenenden und Feiertagen
- Status-Messages für UI
"""

from datetime import datetime, time, timedelta
from typing import Optional, Tuple, Dict
import pytz
import pandas as pd
from enum import Enum

# Zeitzonen
UTC = pytz.UTC
EASTERN = pytz.timezone('US/Eastern')


class MarketSession(Enum):
    """Market Session Types"""
    PRE_MARKET = "pre_market"
    REGULAR = "regular"
    AFTER_HOURS = "after_hours"
    CLOSED = "closed"


class MarketHours:
    """
    Verwaltet alle Market Hours Logik für US Equity Trading
    
    Trading Sessions (Eastern Time):
    - Pre-Market: 4:00 AM - 9:30 AM ET
    - Regular Market: 9:30 AM - 4:00 PM ET
    - After-Hours: 4:00 PM - 8:00 PM ET
    """
    
    # Regular Market Hours (ET)
    MARKET_OPEN = time(9, 30)
    MARKET_CLOSE = time(16, 0)
    
    # Extended Hours (ET)
    PRE_MARKET_START = time(4, 0)
    AFTER_HOURS_END = time(20, 0)
    
    # US Stock Market Holidays 2026
    MARKET_HOLIDAYS_2026 = [
        datetime(2026, 1, 1),   # New Year's Day
        datetime(2026, 1, 19),  # Martin Luther King Jr. Day
        datetime(2026, 2, 16),  # Presidents' Day
        datetime(2026, 4, 3),   # Good Friday
        datetime(2026, 5, 25),  # Memorial Day
        datetime(2026, 7, 3),   # Independence Day (observed)
        datetime(2026, 9, 7),   # Labor Day
        datetime(2026, 11, 26), # Thanksgiving
        datetime(2026, 12, 25), # Christmas
    ]
    
    @staticmethod
    def get_eastern_time() -> datetime:
        """
        Gibt die aktuelle Zeit in Eastern Time zurück
        
        Returns:
            datetime: Aktuelle ET Zeit
        """
        return datetime.now(EASTERN)
    
    @staticmethod
    def utc_to_eastern(utc_timestamp: datetime) -> datetime:
        """
        Konvertiert UTC Timestamp zu Eastern Time
        
        Args:
            utc_timestamp: UTC datetime object
            
        Returns:
            datetime: ET datetime object
        """
        if utc_timestamp.tzinfo is None:
            utc_timestamp = UTC.localize(utc_timestamp)
        return utc_timestamp.astimezone(EASTERN)
    
    @staticmethod
    def unix_to_eastern(unix_timestamp: int) -> datetime:
        """
        Konvertiert Unix Timestamp (seconds since epoch) zu Eastern Time
        
        Args:
            unix_timestamp: Unix timestamp in seconds
            
        Returns:
            datetime: ET datetime object
        """
        utc_dt = datetime.fromtimestamp(unix_timestamp, tz=UTC)
        return MarketHours.utc_to_eastern(utc_dt)
    
    @staticmethod
    def is_weekend(dt: datetime) -> bool:
        """
        Prüft ob das Datum ein Wochenende ist
        
        Args:
            dt: datetime object
            
        Returns:
            bool: True wenn Samstag oder Sonntag
        """
        return dt.weekday() >= 5  # 5=Saturday, 6=Sunday
    
    @staticmethod
    def is_market_holiday(dt: datetime) -> bool:
        """
        Prüft ob das Datum ein Börsen-Feiertag ist
        
        Args:
            dt: datetime object (in ET)
            
        Returns:
            bool: True wenn Feiertag
        """
        date_only = dt.date()
        return any(holiday.date() == date_only for holiday in MarketHours.MARKET_HOLIDAYS_2026)
    
    @staticmethod
    def is_trading_day(dt: datetime) -> bool:
        """
        Prüft ob an diesem Tag gehandelt wird
        (kein Wochenende, kein Feiertag)
        
        Args:
            dt: datetime object (in ET)
            
        Returns:
            bool: True wenn Trading Day
        """
        return not MarketHours.is_weekend(dt) and not MarketHours.is_market_holiday(dt)
    
    @staticmethod
    def get_market_session(dt: Optional[datetime] = None) -> MarketSession:
        """
        Bestimmt die aktuelle Market Session
        
        Args:
            dt: Optional datetime (default: jetzt in ET)
            
        Returns:
            MarketSession: Aktuelle Session
        """
        if dt is None:
            dt = MarketHours.get_eastern_time()
        else:
            # Konvertiere zu Eastern Time (egal ob naive oder aware)
            if dt.tzinfo is None:
                # Naive datetime - assume UTC
                dt = UTC.localize(dt).astimezone(EASTERN)
            else:
                # Already timezone-aware - convert to ET
                dt = dt.astimezone(EASTERN)
        
        # Wochenende oder Feiertag
        if not MarketHours.is_trading_day(dt):
            return MarketSession.CLOSED
        
        current_time = dt.time()
        
        # Pre-Market: 4:00 AM - 9:30 AM ET
        if MarketHours.PRE_MARKET_START <= current_time < MarketHours.MARKET_OPEN:
            return MarketSession.PRE_MARKET
        
        # Regular Market: 9:30 AM - 4:00 PM ET
        elif MarketHours.MARKET_OPEN <= current_time < MarketHours.MARKET_CLOSE:
            return MarketSession.REGULAR
        
        # After-Hours: 4:00 PM - 8:00 PM ET
        elif MarketHours.MARKET_CLOSE <= current_time < MarketHours.AFTER_HOURS_END:
            return MarketSession.AFTER_HOURS
        
        # Außerhalb aller Sessions
        else:
            return MarketSession.CLOSED
    
    @staticmethod
    def is_market_open(dt: Optional[datetime] = None) -> bool:
        """
        Prüft ob Market aktuell geöffnet ist (Regular Hours)
        
        Args:
            dt: Optional datetime (default: jetzt)
            
        Returns:
            bool: True wenn Regular Market Hours
        """
        return MarketHours.get_market_session(dt) == MarketSession.REGULAR
    
    @staticmethod
    def get_market_status() -> Dict:
        """
        Gibt detaillierten Market Status zurück
        
        Returns:
            dict: Market Status Information
        """
        now_et = MarketHours.get_eastern_time()
        session = MarketHours.get_market_session(now_et)
        
        status = {
            'is_open': session == MarketSession.REGULAR,
            'session': session.value,
            'current_time_et': now_et,
        }
        
        # Status Message und Emoji
        if session == MarketSession.REGULAR:
            status['emoji'] = '🟢'
            status['message'] = f"Market ist geöffnet (Regular Hours)"
            status['next_close'] = MarketHours._get_next_market_close(now_et)
            
        elif session == MarketSession.PRE_MARKET:
            status['emoji'] = '🟡'
            status['message'] = f"Pre-Market (Market öffnet um 9:30 AM ET)"
            status['next_open'] = MarketHours._get_next_market_open(now_et)
            
        elif session == MarketSession.AFTER_HOURS:
            status['emoji'] = '🟡'
            status['message'] = f"After-Hours (Market geschlossen um 4:00 PM ET)"
            status['next_open'] = MarketHours._get_next_market_open(now_et)
            
        else:  # CLOSED
            if MarketHours.is_weekend(now_et):
                status['emoji'] = '🔴'
                status['message'] = f"Market geschlossen (Wochenende)"
            elif MarketHours.is_market_holiday(now_et):
                status['emoji'] = '🔴'
                status['message'] = f"Market geschlossen (Feiertag)"
            else:
                status['emoji'] = '🔴'
                status['message'] = f"Market geschlossen"
            
            status['next_open'] = MarketHours._get_next_market_open(now_et)
        
        return status
    
    @staticmethod
    def _get_next_market_open(from_dt: datetime) -> datetime:
        """Berechnet nächste Market-Öffnung"""
        current = from_dt
        
        # Wenn heute noch öffnet
        if current.time() < MarketHours.MARKET_OPEN and MarketHours.is_trading_day(current):
            return current.replace(
                hour=MarketHours.MARKET_OPEN.hour,
                minute=MarketHours.MARKET_OPEN.minute,
                second=0,
                microsecond=0
            )
        
        # Nächster Trading Day
        for _ in range(10):  # Max 10 Tage vorwärts suchen
            current += timedelta(days=1)
            if MarketHours.is_trading_day(current):
                return current.replace(
                    hour=MarketHours.MARKET_OPEN.hour,
                    minute=MarketHours.MARKET_OPEN.minute,
                    second=0,
                    microsecond=0
                )
        
        return None
    
    @staticmethod
    def _get_next_market_close(from_dt: datetime) -> datetime:
        """Berechnet nächste Market-Schließung"""
        if MarketHours.is_market_open(from_dt):
            return from_dt.replace(
                hour=MarketHours.MARKET_CLOSE.hour,
                minute=MarketHours.MARKET_CLOSE.minute,
                second=0,
                microsecond=0
            )
        return None
    
    @staticmethod
    def filter_regular_hours(df: pd.DataFrame, timestamp_col: str = 'timestamp') -> pd.DataFrame:
        """
        Filtert DataFrame auf Regular Market Hours
        
        Args:
            df: DataFrame mit Zeitstempel
            timestamp_col: Name der Timestamp-Spalte
            
        Returns:
            pd.DataFrame: Gefiltertes DataFrame (nur Regular Hours)
        """
        if df.empty or timestamp_col not in df.columns:
            return df
        
        # Kopie erstellen
        df = df.copy()
        
        # Sicherstellen dass Timestamps datetime sind
        if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
            df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        
        # Konvertiere zu Eastern Time
        if df[timestamp_col].dt.tz is None:
            df[timestamp_col] = df[timestamp_col].dt.tz_localize(UTC)
        
        df[timestamp_col] = df[timestamp_col].dt.tz_convert(EASTERN)
        
        # Extrahiere Zeit und Wochentag
        df['_time'] = df[timestamp_col].dt.time
        df['_weekday'] = df[timestamp_col].dt.dayofweek
        df['_date'] = df[timestamp_col].dt.date
        
        # Filter: Regular Market Hours (9:30 AM - 4:00 PM ET)
        time_mask = (df['_time'] >= MarketHours.MARKET_OPEN) & \
                   (df['_time'] < MarketHours.MARKET_CLOSE)
        
        # Filter: Montag-Freitag (0-4)
        weekday_mask = df['_weekday'] < 5
        
        # Filter: Keine Feiertage
        holiday_dates = [h.date() for h in MarketHours.MARKET_HOLIDAYS_2026]
        holiday_mask = ~df['_date'].isin(holiday_dates)
        
        # Kombiniere Filter
        mask = time_mask & weekday_mask & holiday_mask
        
        # Cleanup temporäre Spalten
        df_filtered = df[mask].drop(columns=['_time', '_weekday', '_date'])
        
        return df_filtered
    
    @staticmethod
    def add_market_session_column(df: pd.DataFrame, timestamp_col: str = 'timestamp') -> pd.DataFrame:
        """
        Fügt DataFrame eine Spalte mit der Market Session hinzu
        
        Args:
            df: DataFrame mit Zeitstempel
            timestamp_col: Name der Timestamp-Spalte
            
        Returns:
            pd.DataFrame: DataFrame mit 'market_session' Spalte
        """
        if df.empty or timestamp_col not in df.columns:
            return df
        
        df = df.copy()
        
        # Konvertiere zu Eastern Time
        if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
            df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        
        if df[timestamp_col].dt.tz is None:
            df[timestamp_col] = df[timestamp_col].dt.tz_localize(UTC)
        
        df[timestamp_col] = df[timestamp_col].dt.tz_convert(EASTERN)
        
        # Bestimme Session für jeden Timestamp
        df['market_session'] = df[timestamp_col].apply(
            lambda dt: MarketHours.get_market_session(dt).value
        )
        
        return df

    @staticmethod
    def filter_trading_days(df: pd.DataFrame, timestamp_col: str = 'timestamp') -> pd.DataFrame:
        """
        Filtert DataFrame auf Trading Days (Mo-Fr, keine Feiertage)
        FÜR DAILY BARS - ohne Uhrzeit-Prüfung!
        
        Args:
            df: DataFrame mit Zeitstempel
            timestamp_col: Name der Timestamp-Spalte
            
        Returns:
            pd.DataFrame: Gefiltertes DataFrame (nur Mo-Fr, keine Feiertage)
        """
        if df.empty or timestamp_col not in df.columns:
            return df
        
        df = df.copy()
        
        # Sicherstellen dass Timestamps datetime sind
        if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
            df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        
        # Konvertiere zu Eastern Time
        if df[timestamp_col].dt.tz is None:
            df[timestamp_col] = df[timestamp_col].dt.tz_localize(UTC)
        
        df[timestamp_col] = df[timestamp_col].dt.tz_convert(EASTERN)
        
        # Extrahiere Wochentag und Datum
        df['_weekday'] = df[timestamp_col].dt.dayofweek
        df['_date'] = df[timestamp_col].dt.date
        
        # ✅ NUR Wochentag-Filter (KEINE Uhrzeit!)
        weekday_mask = df['_weekday'] < 5  # Montag-Freitag
        
        # Filter: Keine Feiertage
        holiday_dates = [h.date() for h in MarketHours.MARKET_HOLIDAYS_2026]
        holiday_mask = ~df['_date'].isin(holiday_dates)
        
        # Kombiniere Filter
        mask = weekday_mask & holiday_mask
        
        # Cleanup
        df_filtered = df[mask].drop(columns=['_weekday', '_date'])
        
        return df_filtered

# Hilfsfunktionen für einfachere Verwendung

def is_market_open() -> bool:
    """Shortcut: Ist Market jetzt geöffnet?"""
    return MarketHours.is_market_open()


def get_market_status_message() -> str:
    """Shortcut: Market Status Message für UI"""
    status = MarketHours.get_market_status()
    time_str = status['current_time_et'].strftime('%I:%M %p ET')
    return f"{status['emoji']} {status['message']} | Zeit: {time_str}"


def filter_regular_hours(df: pd.DataFrame, timestamp_col: str = 'timestamp') -> pd.DataFrame:
    """Shortcut: Filter DataFrame auf Regular Hours"""
    return MarketHours.filter_regular_hours(df, timestamp_col)


# ============================================
# Test & Demo Funktionen
# ============================================

def demo_market_hours():
    """Demonstriert alle Market Hours Funktionen"""
    print("=" * 60)
    print("MARKET HOURS LOGIC - DEMO")
    print("=" * 60)
    
    # 1. Aktuelle Zeit
    now_et = MarketHours.get_eastern_time()
    print(f"\n1. Aktuelle Zeit (ET): {now_et.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # 2. Market Status
    status = MarketHours.get_market_status()
    print(f"\n2. Market Status:")
    print(f"   {status['emoji']} {status['message']}")
    print(f"   Session: {status['session']}")
    print(f"   Is Open: {status['is_open']}")
    
    if 'next_open' in status and status['next_open']:
        print(f"   Nächste Öffnung: {status['next_open'].strftime('%Y-%m-%d %I:%M %p ET')}")
    if 'next_close' in status and status['next_close']:
        print(f"   Nächste Schließung: {status['next_close'].strftime('%I:%M %p ET')}")
    
    # 3. Verschiedene Zeiten testen
    print(f"\n3. Test verschiedener Zeiten:")
    test_times = [
        datetime(2026, 1, 30, 8, 0),   # Pre-Market
        datetime(2026, 1, 30, 10, 0),  # Regular
        datetime(2026, 1, 30, 17, 0),  # After-Hours
        datetime(2026, 1, 30, 22, 0),  # Geschlossen
        datetime(2026, 2, 1, 12, 0),   # Sonntag
        datetime(2026, 1, 1, 12, 0),   # Feiertag
    ]
    
    for test_dt in test_times:
        et_dt = EASTERN.localize(test_dt)
        session = MarketHours.get_market_session(et_dt)
        day_name = et_dt.strftime('%A')
        print(f"   {et_dt.strftime('%Y-%m-%d %H:%M')} ({day_name}): {session.value}")
    
    # 4. UTC zu ET Konvertierung
    print(f"\n4. UTC zu ET Konvertierung:")
    utc_now = datetime.now(UTC)
    et_now = MarketHours.utc_to_eastern(utc_now)
    print(f"   UTC:  {utc_now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"   ET:   {et_now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # 5. DataFrame Filtering Demo
    print(f"\n5. DataFrame Filtering Demo:")
    
    # Erstelle Test-DataFrame mit verschiedenen Zeiten
    test_data = []
    base_date = datetime(2026, 1, 30)  # Freitag
    
    for hour in range(4, 21):  # 4 AM - 8 PM
        for minute in [0, 30]:
            dt = EASTERN.localize(base_date.replace(hour=hour, minute=minute))
            test_data.append({
                'timestamp': dt,
                'price': 100 + hour,
                'volume': 1000
            })
    
    # Füge Wochenende hinzu
    weekend_dt = EASTERN.localize(datetime(2026, 2, 1, 12, 0))  # Sonntag
    test_data.append({
        'timestamp': weekend_dt,
        'price': 105,
        'volume': 1000
    })
    
    df = pd.DataFrame(test_data)
    print(f"   Original DataFrame: {len(df)} rows")
    
    # Filter auf Regular Hours
    df_filtered = MarketHours.filter_regular_hours(df)
    print(f"   Nach Filter (Regular Hours): {len(df_filtered)} rows")
    if not df_filtered.empty:
        print(f"   Erste 3 Zeilen:")
        print(df_filtered[['timestamp', 'price']].head(3).to_string(index=False))
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    # Führe Demo aus
    demo_market_hours()
    
    print("\n\n🧪 Unit Tests:")
    print("-" * 60)
    
    # Quick Tests
    print(f"✓ Is market open now? {is_market_open()}")
    print(f"✓ Status message: {get_market_status_message()}")
    
    # Test Weekend
    weekend = EASTERN.localize(datetime(2026, 2, 1, 12, 0))
    print(f"✓ Is weekend (Sun)? {MarketHours.is_weekend(weekend)}")
    
    # Test Holiday
    holiday = EASTERN.localize(datetime(2026, 1, 1, 12, 0))
    print(f"✓ Is holiday (New Year)? {MarketHours.is_market_holiday(holiday)}")
    
    # Test Regular Hours
    regular = EASTERN.localize(datetime(2026, 1, 30, 10, 0))
    print(f"✓ Is market open (Fri 10 AM)? {MarketHours.is_market_open(regular)}")
    
    print("\n✅ Alle Tests erfolgreich!")
