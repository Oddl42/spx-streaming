# src/streaming/websocket_client.py
"""
Massive.com WebSocket Client für Stock Data Streaming
Unterstützt Minuten- und Sekunden-Aggregation
"""

import os
import asyncio
import json
from typing import List, Callable, Optional, Dict
from datetime import datetime
from collections import deque
import threading
from dotenv import load_dotenv

try:
    from massive import WebSocketClient as MassiveWSClient
    from massive.websocket.models import WebSocketMessage, Feed, Market
    MASSIVE_AVAILABLE = True
except ImportError:
    print("⚠️ 'massive' package nicht gefunden. Installiere mit: pip install massive")
    MASSIVE_AVAILABLE = False

load_dotenv()


class StockWebSocketClient:
    """
    WebSocket Client für Live Stock Streaming
    
    Unterstützt:
    - Minuten-Aggregation (AM.*)
    - Sekunden-Aggregation (A.*)
    - Multiple Ticker gleichzeitig
    - Callback-basierte Datenverarbeitung
    """
    
    def __init__(
        self, 
        api_key: Optional[str] = None,
        aggregation: str = 'minute',
        feed: str = 'delayed'
    ):
        """
        Args:
            api_key: Massive.com API Key (oder aus .env)
            aggregation: 'minute' oder 'second'
            feed: 'delayed' oder 'realtime'
        """
        self.api_key = api_key or os.getenv('MASSIVE_API_KEY')
        if not self.api_key:
            raise ValueError("API Key nicht gefunden! Setze MASSIVE_API_KEY in .env")
        
        self.aggregation = aggregation
        self.feed_type = Feed.Delayed if feed == 'delayed' else Feed.RealTime
        self.feed_type = Feed.Delayed
        
        # Client
        self.client: Optional[MassiveWSClient] = None
        self.is_connected = False
        self.is_running = False
        
        # Ticker Management
        self.subscribed_tickers: List[str] = []
        
        # Callback
        self.message_callback: Optional[Callable] = None
        
        # Threading
        self.thread: Optional[threading.Thread] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop_event = threading.Event()
        
        print(f"✓ WebSocket Client initialisiert (Aggregation: {aggregation})")
    
    def connect(self):
        """Initialisiert WebSocket Client"""
        if not MASSIVE_AVAILABLE:
            raise RuntimeError("massive package nicht installiert!")
        
        try:
            self.client = MassiveWSClient(
                api_key=self.api_key,
                #feed=self.feed_type,
                feed=Feed.Delayed,
                market=Market.Stocks
            )
            self.is_connected = True
            print("✓ WebSocket Client verbunden")
        except Exception as e:
            print(f"✗ WebSocket Verbindung fehlgeschlagen: {e}")
            raise
    
    def subscribe(self, tickers: List[str]):
        """
        Abonniert Ticker für Live-Updates
        
        Args:
            tickers: Liste von Ticker-Symbolen (z.B. ['AAPL', 'MSFT'])
        """
        if not self.client:
            self.connect()
        
        self.subscribed_tickers = tickers
        
        # Subscription Patterns
        if self.aggregation == 'minute':
            # Minuten: AM.TICKER
            patterns = [f"AM.{ticker}" for ticker in tickers]
        else:
            # Sekunden: A.TICKER (NICHT AS.* !)
            patterns = [f"A.{ticker}" for ticker in tickers]
        
        print(f"✓ Abonniere {len(patterns)} Ticker: {', '.join(tickers[:5])}...")
        print(f"   Patterns: {patterns[:3]}...")
        
        try:
            # Subscribe
            self.client.subscribe(*patterns)
            self.client.subscribe("AM.AAPL","AM.MSFT")
            print(f"✓ {len(patterns)} Subscriptions erfolgreich")
        except Exception as e:
            print(f"✗ Subscription fehlgeschlagen: {e}")
            raise
    
    def set_message_callback(self, callback: Callable[[Dict], None]):
        """Setzt Callback-Funktion für eingehende Nachrichten"""
        self.message_callback = callback
    
    def _handle_messages(self, messages: List[WebSocketMessage]):
        """Interne Message Handler"""
        for msg in messages:
            try:
                data = self._parse_message(msg)
                
                if self.message_callback:
                    self.message_callback(data)
                    
            except Exception as e:
                print(f"✗ Fehler beim Verarbeiten: {e}")
    
    def _parse_message(self, msg: WebSocketMessage) -> Dict:
        """Parsed WebSocket Message zu Dictionary"""
        try:
            data = {
                'event_type': getattr(msg, 'ev', 'unknown'),
                'symbol': getattr(msg, 'sym', None),
                'timestamp': getattr(msg, 's', None),  # Start timestamp (ms)
                'open': getattr(msg, 'o', None),
                'high': getattr(msg, 'h', None),
                'low': getattr(msg, 'l', None),
                'close': getattr(msg, 'c', None),
                'volume': getattr(msg, 'v', None),
                'vwap': getattr(msg, 'vw', None)
                #'transactions': getattr(msg, 'n', None),
            }
            return data
        except Exception as e:
            print(f"✗ Parse Error: {e}")
            return {}
    
    def start(self):
        """Startet WebSocket Stream in separatem Thread"""
        if self.is_running:
            print("⚠️ Stream läuft bereits")
            return
        
        if not self.client:
            self.connect()
        
        if not self.subscribed_tickers:
            print("⚠️ Keine Ticker abonniert. Rufe subscribe() zuerst auf.")
            return
        
        self.is_running = True
        self._stop_event.clear()
        
        # Starte in separatem Thread
        self.thread = threading.Thread(target=self._run_stream, daemon=True)
        self.thread.start()
        
        print("✓ WebSocket Stream gestartet")
    
    def _run_stream(self):
        """Run Stream in Thread"""
        try:
            print("📡 Starte WebSocket Streaming...")
            
            # Run mit timeout
            self.client.run(self._handle_messages)
            
        except Exception as e:
            print(f"✗ Stream Error: {e}")
            self.is_running = False
    
    def stop(self):
        """Stoppt WebSocket Stream"""
        if not self.is_running:
            return
        
        print("🛑 Stoppe WebSocket Stream...")
        self.is_running = False
        self._stop_event.set()
        
        # ✅ FIX: Close async richtig
        if self.client:
            try:
                # Versuche zuerst disconnect
                if hasattr(self.client, 'disconnect'):
                    self.client.disconnect()
                # Falls close existiert und nicht async ist
                elif hasattr(self.client, 'close'):
                    close_method = self.client.close
                    # Prüfe ob es eine Coroutine ist
                    if asyncio.iscoroutinefunction(close_method):
                        # Erstelle neuen Event Loop für Cleanup
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            loop.run_until_complete(close_method())
                        finally:
                            loop.close()
                    else:
                        close_method()
            except Exception as e:
                print(f"⚠️ Close Warning: {e}")
        
        self.is_connected = False
        self.client = None
        
        # Warte auf Thread
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2.0)
        
        print("✓ WebSocket Stream gestoppt")
    
    def get_status(self) -> Dict:
        """Gibt aktuellen Status zurück"""
        return {
            'connected': self.is_connected,
            'running': self.is_running,
            'aggregation': self.aggregation,
            'subscribed_tickers': len(self.subscribed_tickers),
            'tickers': self.subscribed_tickers[:10]
        }


# ============================================
# Test
# ============================================

def test_websocket():
    """Test WebSocket Client"""
    print("=" * 60)
    print("WEBSOCKET CLIENT TEST")
    print("=" * 60)
    
    message_count = {'count': 0}
    
    def message_handler(data: Dict):
        """Handler für Test"""
        message_count['count'] += 1
        
        if message_count['count'] <= 5:
            print(f"\n📨 Message #{message_count['count']}:")
            print(f"   Symbol: {data['symbol']}")
            print(f"   Time: {data['timestamp']}")
            if data['close']:
                print(f"   Close: ${data['close']:.2f}")
            if data['volume']:
                print(f"   Volume: {data['volume']:,}")
    
    # Client erstellen
    client = StockWebSocketClient(aggregation='minute')
    client.set_message_callback(message_handler)
    
    # Ticker abonnieren
    test_tickers = ['AAPL', 'MSFT']
    #test_tickers = ['AAPL']
    client.subscribe(test_tickers)
    
    # Stream starten
    client.start()
    
    try:
        print("\n⏳ Warte auf Messages (30 Sekunden)...")
        import time
        time.sleep(30)
        
        print(f"\n✓ Test abgeschlossen: {message_count['count']} Messages empfangen")
        
    except KeyboardInterrupt:
        print("\n\n⏹️ Test manuell beendet")
    finally:
        client.stop()


if __name__ == "__main__":
    test_websocket()
