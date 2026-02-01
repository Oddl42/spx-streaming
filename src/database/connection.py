#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 31 17:18:07 2026

@author: twi-dev
"""

# src/database/connection.py
"""
Database Connection Manager für TimescaleDB
"""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager
from typing import Generator
from dotenv import load_dotenv

from src.database.models import Base

load_dotenv()


class DatabaseManager:
    """
    Verwaltet Datenbank-Verbindungen zu TimescaleDB
    """
    
    def __init__(self, database_url: str = None):
        """
        Args:
            database_url: PostgreSQL Connection String
                         Format: postgresql://user:pass@host:port/dbname
        """
        self.database_url = database_url or os.getenv('DATABASE_URL')
        
        if not self.database_url:
            self.database_url = "postgresql://sp500user:sp500pass@localhost:5432/sp500_data"
        
        # Engine mit Connection Pooling
        self.engine = create_engine(
            self.database_url,
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,  # Prüft Connection vor Verwendung
            echo=False  # SQL Logging (setze auf True für Debug)
        )
        
        # Session Factory
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )
    
    def create_tables(self):
        """Erstellt alle Tabellen (falls nicht in init.sql gemacht)"""
        Base.metadata.create_all(bind=self.engine)
        print("✓ Tabellen erstellt")
    
    def test_connection(self) -> bool:
        """Testet Datenbank-Verbindung"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                version = conn.execute(text("SELECT version()")).fetchone()
                print(f"✓ Datenbank-Verbindung erfolgreich")
                print(f"  PostgreSQL Version: {version[0][:50]}...")
                return True
        except Exception as e:
            print(f"✗ Datenbank-Verbindung fehlgeschlagen: {e}")
            return False
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """
        Context Manager für Session
        
        Usage:
            with db_manager.get_session() as session:
                session.query(...)
        """
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def get_stats(self) -> dict:
        """Gibt Datenbank-Statistiken zurück"""
        with self.get_session() as session:
            # Anzahl Ticker
            ticker_count = session.execute(
                text("SELECT COUNT(*) FROM tickers")
            ).scalar()
            
            # Anzahl Daily Bars
            daily_count = session.execute(
                text("SELECT COUNT(*) FROM daily_bars")
            ).scalar()
            
            # Anzahl Minute Bars
            minute_count = session.execute(
                text("SELECT COUNT(*) FROM minute_bars")
            ).scalar()
            
            # Datenbankgröße
            db_size = session.execute(
                text("SELECT pg_size_pretty(pg_database_size(current_database()))")
            ).scalar()
            
            return {
                'tickers': ticker_count,
                'daily_bars': daily_count,
                'minute_bars': minute_count,
                'database_size': db_size
            }
    
    def close(self):
        """Schließt Datenbank-Verbindungen"""
        self.engine.dispose()
        print("✓ Datenbank-Verbindungen geschlossen")


# Singleton Instance
db_manager = DatabaseManager()


# Test
if __name__ == "__main__":
    print("=== Database Connection Test ===\n")
    
    # Test Connection
    if db_manager.test_connection():
        print("\n=== Database Stats ===")
        stats = db_manager.get_stats()
        for key, value in stats.items():
            print(f"{key}: {value}")
