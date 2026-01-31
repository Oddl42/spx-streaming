#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 31 16:46:45 2026

@author: twi-dev
"""

# src/streaming/websocket_client_direct.py
"""
Direkte WebSocket Implementation für Massive.com
Umgeht Probleme mit der massive Library
"""

import os
import asyncio
import websockets
import json
from typing import List, Callable, Optional, Dict
from datetime import datetime
import threading
from dotenv import load_dotenv
import time

load_dotenv()


class DirectMassiveWebSocketClient:
    """
    Direkter WebSocket Client für Massive.com
    Verwendet websockets Library direkt
    """
    
    # Massive.com WebSocket Endpoints
    WS_URL_DELAYED = "wss://socket.polygon.io/stocks"
    WS_URL_REALTIME = "wss://socket.polygon.io/stocks/realtime"
    
    def __init__(
        self, 
        api_key: Optional[str] = None,
        aggregation: str = 'minute',
        feed: str = 'delayed'
    ):
        """
        Args:
            api_key: Massive.com/Polygon.io API Key
            aggregation: 'minute' oder 'second'
            feed: 'delayed' oder 'realtime'
        """
        self.api_key = api_key or os.getenv('MASSIVE_API_KEY')
        if not self.api_key:
            raise ValueError("API Key nicht gefunden!")
        
        self.aggregation = aggregation
        self.ws_url = self.WS_URL_DELAYED if feed == 'delayed' else self.WS_URL_REALTIME
        
        self.subscribed_tickers: List[str] = []
        self.message_callback: Optional[Callable] = None
        
        self.is_running = False
        self.websocket = None
        self.thread: Optional[threading.Thread] = None
        
        print(f"✓ Direct WebSocket Client (Aggregation: {aggregation}, Feed: {feed})")
    
    def subscribe(self, tickers: List[str]):
        """Setzt Ticker für Subscription"""
        self.subscribed_tickers = tickers
        print(f"✓ {len(tickers)} Ticker vorbereitet: {', '.join(tickers[:3])}...")
    
    def set_message_callback(self, callback: Callable[[Dict], None]):
        """Setzt Message Callback"""
        self.message_callback = callback
    
    async def _connect_and_stream(self):
        """Async WebSocket Connection & Streaming"""
        try:
            print(f"🔌 Verbinde zu {self.ws_url}...")
            
            async with websockets.connect(
                self.ws_url,
                ping_interval=20,
                ping_timeout=10
            ) as websocket:
                self.websocket = websocket
                print("✓ WebSocket verbunden")
                
                # 1. Warte auf Connection Successful Message
                welcome_msg = await websocket.recv()
                print(f"📨 Welcome: {welcome_msg}")
                
                # 2. Authentifizierung
                auth_msg = {"action": "auth", "params": self.api_key}
                await websocket.send(json.dumps(auth_msg))
                print("🔐 Auth gesendet...")
                
                # Warte auf Auth-Response
                auth_response = await websocket.recv()
                auth_data = json.loads(auth_response)
                print(f"✓ Auth Response: {auth_data}")
                
                if auth_data[0].get('status') != 'auth_success':
                    raise Exception(f"Auth fehlgeschlagen: {auth_data}")
                
                # 3. Subscribe zu Tickern
                if self.aggregation == 'minute':
                    # Minuten-Bars: AM.TICKER
                    channels = [f"AM.{ticker}" for ticker in self.subscribed_tickers]
                else:
                    # Sekunden-Aggregation: A.TICKER
                    channels = [f"A.{ticker}" for ticker in self.subscribed_tickers]
                
                sub_msg = {"action": "subscribe", "params": ",".join(channels)}
                await websocket.send(json.dumps(sub_msg))
                print(f"📡 Subscribed: {channels}")
                
                # Warte auf Subscription Confirmation
                sub_response = await websocket.recv()
                print(f"✓ Subscription Response: {sub_response}")
                
                # 4. Message Loop
                print("👂 Warte auf Messages...")
                message_count = 0
                
                while self.is_running:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                        
                        # Parse Message
                        data = json.loads(message)
                        
                        # Verarbeite nur relevante Messages (Type: AM oder A)
                        if isinstance(data, list):
                            for item in data:
                                if item.get('ev') in ['AM', 'A']:
                                    message_count += 1
                                    
                                    # Konvertiere zu Standard-Format
                                    parsed = {
                                        'event_type': item.get('ev'),
                                        'symbol': item.get('sym'),
                                        'timestamp': item.get('s'),  # Start timestamp (ms)
                                        'open': item.get('o'),
                                        'high': item.get('h'),
                                        'low': item.get('l'),
                                        'close': item.get('c'),
                                        'volume': item.get('v'),
                                        'vwap': item.get('vw'),
                                        'transactions': item.get('n'),
                                    }
                                    
                                    if message_count <= 5:
                                        print(f"📊 Message #{message_count}: {parsed['symbol']} @ ${parsed['close']}")
                                    
                                    # Callback
                                    if self.message_callback:
                                        self.message_callback(parsed)
                    
                    except asyncio.TimeoutError:
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        print("⚠️ Verbindung geschlossen")
                        break
                
                print(f"✓ Streaming beendet ({message_count} Messages empfangen)")
        
        except Exception as e:
            print(f"✗ WebSocket Error: {e}")
            import traceback
            traceback.print_exc()
    
    def start(self):
        """Startet WebSocket Stream"""
        if self.is_running:
            print("⚠️ Stream läuft bereits")
            return
        
        self.is_running = True
        self.thread = threading.Thread(target=self._run_async_loop, daemon=True)
        self.thread.start()
        print("✓ WebSocket Stream Thread gestartet")
    
    def _run_async_loop(self):
        """Run Async Loop in Thread"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._connect_and_stream())
        finally:
            loop.close()
    
    def stop(self):
        """Stoppt WebSocket Stream"""
        if not self.is_running:
            return
        
        print("🛑 Stoppe Stream...")
        self.is_running = False
        
        # Warte auf Thread
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=3.0)
        
        print("✓ Stream gestoppt")
    
    def get_status(self) -> Dict:
        """Gibt Status zurück"""
        return {
            'running': self.is_running,
            'aggregation': self.aggregation,
            'subscribed_tickers': len(self.subscribed_tickers),
            'tickers': self.subscribed_tickers[:5]
        }


# ============================================
# Test
# ============================================

def test_direct_websocket():
    """Test Direct WebSocket"""
    print("=" * 60)
    print("DIRECT WEBSOCKET TEST")
    print("=" * 60)
    
    message_count = {'count': 0}
    
    def handler(data: Dict):
        message_count['count'] += 1
        if message_count['count'] <= 10:
            print(f"\n✓ Message #{message_count['count']}:")
            print(f"   {data['symbol']}: ${data.get('close', 'N/A')}")
            print(f"   Vol: {data.get('volume', 'N/A')}")
    
    # Client
    client = DirectMassiveWebSocketClient(aggregation='minute')
    client.set_message_callback(handler)
    
    # Subscribe (nur 1 Ticker für Test)
    client.subscribe(['AAPL'])
    
    # Start
    client.start()
    
    try:
        print("\n⏳ Warte 60 Sekunden...")
        time.sleep(60)
    except KeyboardInterrupt:
        print("\n⏹️ Abgebrochen")
    finally:
        client.stop()
        print(f"\n📊 Gesamt: {message_count['count']} Messages")


if __name__ == "__main__":
    # Installiere websockets falls nötig
    try:
        import websockets
    except ImportError:
        print("⚠️ websockets nicht installiert!")
        print("Installiere mit: pip install websockets")
        exit(1)
    
    test_direct_websocket()
