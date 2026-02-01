#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 31 17:17:41 2026

@author: twi-dev
"""

# src/database/models.py
"""
SQLAlchemy Models für TimescaleDB
S&P 500 Stock Data
"""

from sqlalchemy import Column, String, Integer, BigInteger, Float, Date, DateTime, Text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class Ticker(Base):
    """S&P 500 Ticker Metadata"""
    __tablename__ = 'tickers'
    
    symbol = Column(String(10), primary_key=True)
    security = Column(String(255), nullable=False)
    gics_sector = Column(String(100))
    gics_sub_industry = Column(String(100))
    headquarters_location = Column(String(255))
    date_added = Column(Date)
    cik = Column(Integer)
    founded = Column(String(50))
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    def __repr__(self):
        return f"<Ticker(symbol='{self.symbol}', security='{self.security}')>"


class DailyBar(Base):
    """Daily OHLCV Bars (seit 2020)"""
    __tablename__ = 'daily_bars'
    
    time = Column(TIMESTAMP(timezone=True), primary_key=True)
    symbol = Column(String(10), primary_key=True)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(BigInteger, nullable=False)
    vwap = Column(Float)
    transactions = Column(Integer)
    
    def __repr__(self):
        return f"<DailyBar(symbol='{self.symbol}', time='{self.time}', close={self.close})>"


class MinuteBar(Base):
    """Minute OHLCV Bars (seit 2023)"""
    __tablename__ = 'minute_bars'
    
    time = Column(TIMESTAMP(timezone=True), primary_key=True)
    symbol = Column(String(10), primary_key=True)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(BigInteger, nullable=False)
    vwap = Column(Float)
    transactions = Column(Integer)
    
    def __repr__(self):
        return f"<MinuteBar(symbol='{self.symbol}', time='{self.time}', close={self.close})>"


class DownloadStatus(Base):
    """Download Progress Tracking"""
    __tablename__ = 'download_status'
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String(10), nullable=False)
    timespan = Column(String(20), nullable=False)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)
    status = Column(String(20), default='pending')
    bars_downloaded = Column(Integer, default=0)
    error_message = Column(Text)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.now)
    
    def __repr__(self):
        return f"<DownloadStatus(symbol='{self.symbol}', status='{self.status}')>"
