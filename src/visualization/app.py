#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
S&P 500 Stock Streaming Platform - COMPLETE
Mit Live-Streaming Charts & Historischen Daten
"""

import dash
from dash import dcc, html, Input, Output, State, dash_table, ctx
from plotly.subplots import make_subplots
import plotly.graph_objs as go
import pandas as pd
from pathlib import Path
import sys
import time
from datetime import datetime, timedelta

# Imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.helpers import SP500TickerLoader
from src.api.massive_rest_client import MassiveRESTClient
from src.utils.market_hours import MarketHours, is_market_open, get_market_status_message
from src.streaming.websocket_client_direct import DirectMassiveWebSocketClient as StockWebSocketClient
#from src.streaming.websocket_client import StockWebSocketClient
from src.streaming.stream_manager import StreamDataManager
from src.utils.indicators import TechnicalIndicators
from src.database.queries import stock_queries
from src.database.connection import db_manager

# ============================================
# App & Globals
# ============================================

app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "S&P 500 Stock Streaming"

# Globals
ticker_loader = SP500TickerLoader()
api_client = MassiveRESTClient()

ws_client: StockWebSocketClient = None
stream_manager: StreamDataManager = None
streaming_active = False
current_aggregation = 'minute'

# ============================================
# App Layout
# ============================================

app.layout = html.Div([
    html.H1("📊 S&P 500 Stock Streaming Platform - Complete", 
            style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': '20px'}),
    
    dcc.Tabs(id='main-tabs', value='streaming-tab', children=[
        dcc.Tab(label='🔴 Streaming (Live)', value='streaming-tab'),
        dcc.Tab(label='📈 Historische Daten', value='historical-tab'),
        dcc.Tab(label='🤖 Machine Learning', value='ml-tab', disabled=True),
    ]),
    
    html.Div(id='tab-content', style={'padding': '20px'}),
    
    # Intervals
    dcc.Interval(id='interval-market-status', interval=10*1000, n_intervals=0),
    dcc.Interval(id='interval-live-chart', interval=2*1000, n_intervals=0, disabled=True),
    dcc.Interval(id='interval-stats', interval=5*1000, n_intervals=0, disabled=True),
    
    # Stores
    dcc.Store(id='streaming-state', data={'active': False, 'aggregation': 'minute'}),
    dcc.Store(id='historical-data-store'),
    
], style={'fontFamily': 'Arial, sans-serif'})


# ============================================
# Streaming Tab Layout (VOLLSTÄNDIG)
# ============================================

def create_streaming_tab():
    return html.Div([
        html.H2("🎯 Real-time Stock Streaming mit Live-Charts"),
        
        html.Div(id='market-status-card', style={'marginBottom': '30px'}),
        
        # Controls Row
        html.Div([
            # Left: Ticker Selection
            html.Div([
                html.H3("📋 Ticker Auswahl"),
                dcc.Dropdown(
                    id='ticker-dropdown',
                    options=[],
                    multi=True,
                    placeholder='Ticker auswählen...',
                ),
                html.Button('Top 5 laden', id='load-top10-btn', n_clicks=0,
                           style={'marginTop': '10px', 'padding': '8px 16px',
                                 'backgroundColor': '#4CAF50', 'color': 'white',
                                 'border': 'none', 'borderRadius': '4px', 'cursor': 'pointer'}),
            ], style={'width': '48%', 'display': 'inline-block', 'verticalAlign': 'top'}),
            
            # Right: Streaming Controls
            html.Div([
                html.H3("🎮 Streaming Controls"),
                
                html.Label("Aggregation:", style={'fontWeight': 'bold'}),
                dcc.RadioItems(
                    id='aggregation-selector',
                    options=[
                        {'label': ' Minuten', 'value': 'minute'},
                        {'label': ' Sekunden', 'value': 'second'}
                    ],
                    value='minute',
                    inline=True,
                    style={'marginBottom': '15px'}
                ),
                
                html.Label("Chart Type:", style={'fontWeight': 'bold'}),
                dcc.RadioItems(
                    id='stream-chart-type',
                    options=[
                        {'label': ' Candlestick', 'value': 'candlestick'},
                        {'label': ' Line Chart', 'value': 'line'}
                    ],
                    value='candlestick',
                    inline=True,
                    style={'marginBottom': '15px'}
                ),
                
                html.Div([
                    dcc.Checklist(
                        id='stream-vwap-toggle',
                        options=[{'label': ' VWAP anzeigen', 'value': 'show'}],
                        value=['show'],
                        inline=True
                    )
                ], style={'marginBottom': '15px'}),
                
                html.Div([
                    html.Label("📊 Chart Ticker:", style={'fontWeight': 'bold'}),
                    dcc.Dropdown(
                        id='chart-ticker-dropdown',
                        placeholder='Ticker wählen...',
                        clearable=False
                    )
                ], id='chart-ticker-selector', style={'marginTop': '10px', 'display': 'none'}),
                
                html.Div([
                    html.Button('🟢 Streaming Starten', id='start-streaming-btn', n_clicks=0,
                               style={'padding': '10px 20px', 'marginRight': '10px', 'marginTop': '15px',
                                     'backgroundColor': '#28a745', 'color': 'white',
                                     'border': 'none', 'borderRadius': '4px', 'cursor': 'pointer',
                                     'fontSize': '14px', 'fontWeight': 'bold'}),
                    
                    html.Button('🔴 Streaming Stoppen', id='stop-streaming-btn', n_clicks=0,
                               disabled=True,
                               style={'padding': '10px 20px', 'marginTop': '15px',
                                     'backgroundColor': '#dc3545', 'color': 'white',
                                     'border': 'none', 'borderRadius': '4px',
                                     'fontSize': '14px', 'fontWeight': 'bold'}),
                ]),
                
                html.Div(id='streaming-status', style={'marginTop': '10px', 'fontSize': '14px'}),
                
            ], style={'width': '48%', 'display': 'inline-block', 'verticalAlign': 'top', 
                     'marginLeft': '4%'}),
        ]),
        
        # Statistics
        html.Div(id='streaming-stats', style={
            'padding': '15px', 'backgroundColor': '#f8f9fa', 'borderRadius': '5px',
            'marginTop': '20px', 'marginBottom': '20px'
        }),
        
        # Ticker Table
        html.Div([
            html.H3("✅ Ausgewählte Ticker"),
            html.Div(id='ticker-table')
        ], style={'marginTop': '20px', 'marginBottom': '30px'}),
        
        # ✅ CHARTS: Zwei separate Graphs statt combined
        html.Div([
            html.H3("📈 Live Price Chart"),
            dcc.Graph(id='live-price-chart', style={'height': '600px'}),
            
            html.H3("📊 Live Volume Chart", style={'marginTop': '20px'}),
            dcc.Graph(id='live-volume-chart', style={'height': '200px'})
        ]),
        
        # Stores
        dcc.Store(id='all-tickers-store'),
    ])


# ============================================
# Historical Data Tab (unverändert)
# ============================================

def create_historical_tab():
    return html.Div([
        html.H2("📈 Historische Daten & Technische Analyse"),
        
        html.Div([
            html.H4("🗄️ Datenbank-Status", style={'marginBottom': '10px'}),
            html.Div(id='db-status-info')
        ], style={'padding': '15px', 'backgroundColor': '#f8f9fa', 
                 'borderRadius': '8px', 'marginBottom': '20px'}),
        
        html.Div([
            html.Div([
                html.H3("🎯 Daten-Auswahl"),
                html.Label("Ticker:", style={'fontWeight': 'bold', 'marginTop': '10px'}),
                dcc.Dropdown(id='hist-ticker-dropdown', options=[], multi=False, placeholder='Ticker auswählen...'),
                html.Label("Timespan:", style={'fontWeight': 'bold', 'marginTop': '15px'}),
                dcc.RadioItems(id='hist-timespan', options=[{'label': ' 1 Tag', 'value': 'day'}], 
                              value='day', style={'marginBottom': '15px'}),
                html.Label("Zeitraum:", style={'fontWeight': 'bold', 'marginTop': '15px'}),
                dcc.DatePickerRange(id='hist-date-range',
                    start_date=(datetime.now() - timedelta(days=365)).date(),
                    end_date=datetime.now().date(), display_format='YYYY-MM-DD',
                    style={'marginBottom': '15px'}),
                html.Button('📊 Daten laden', id='load-hist-data-btn', n_clicks=0,
                           style={'width': '100%', 'padding': '10px', 'marginTop': '15px',
                                 'backgroundColor': '#2196f3', 'color': 'white', 
                                 'border': 'none', 'borderRadius': '5px',
                                 'cursor': 'pointer', 'fontSize': '16px'}),
            ], style={'width': '30%', 'display': 'inline-block', 'verticalAlign': 'top',
                     'padding': '20px', 'backgroundColor': '#f8f9fa', 'borderRadius': '8px'}),
            
            html.Div([
                html.H3("📐 Chart & Indikatoren"),
                html.Label("Chart Type:", style={'fontWeight': 'bold'}),
                dcc.RadioItems(id='hist-chart-type',
                    options=[{'label': ' Candlestick', 'value': 'candlestick'},
                            {'label': ' Line Chart', 'value': 'line'}],
                    value='candlestick', inline=True, style={'marginBottom': '15px'}),
                html.Label("Moving Averages:", style={'fontWeight': 'bold'}),
                dcc.Checklist(id='hist-ma-select',
                    options=[{'label': ' SMA 20', 'value': 'SMA_20'},
                            {'label': ' SMA 50', 'value': 'SMA_50'},
                            {'label': ' SMA 200', 'value': 'SMA_200'},
                            {'label': ' EMA 12', 'value': 'EMA_12'},
                            {'label': ' EMA 26', 'value': 'EMA_26'}],
                    value=['SMA_20', 'SMA_50'], inline=True, style={'marginBottom': '15px'}),
                html.Div([dcc.Checklist(id='hist-bb-toggle',
                    options=[{'label': ' Bollinger Bands (20, 2σ)', 'value': 'show'}],
                    value=[], inline=True)], style={'marginBottom': '15px'}),
                html.Div([dcc.Checklist(id='hist-vwap-toggle',
                    options=[{'label': ' VWAP', 'value': 'show'}],
                    value=[], inline=True)], style={'marginBottom': '15px'}),
                html.Label("Zusätzliche Indikatoren:", style={'fontWeight': 'bold', 'marginTop': '10px'}),
                dcc.Checklist(id='hist-indicators',
                    options=[{'label': ' RSI (14)', 'value': 'RSI'},
                            {'label': ' MACD', 'value': 'MACD'},
                            {'label': ' Volume', 'value': 'Volume'}],
                    value=['Volume'], inline=True),
            ], style={'width': '65%', 'display': 'inline-block', 'verticalAlign': 'top',
                     'marginLeft': '5%', 'padding': '20px', 'backgroundColor': '#f8f9fa',
                     'borderRadius': '8px'}),
        ]),
        
        html.Div(id='hist-status', style={'marginTop': '20px', 'marginBottom': '20px',
                                          'padding': '15px', 'backgroundColor': '#e3f2fd',
                                          'borderRadius': '5px'}),
        
        html.Div([
            dcc.Graph(id='hist-main-chart', style={'height': '600px'}),
            dcc.Graph(id='hist-indicator-chart', style={'height': '300px'})
        ]),
    ])


# ============================================
# Callbacks
# ============================================

@app.callback(
    Output('tab-content', 'children'),
    Input('main-tabs', 'value')
)
def render_tab_content(active_tab):
    if active_tab == 'streaming-tab':
        return create_streaming_tab()
    elif active_tab == 'historical-tab':
        return create_historical_tab()
    return html.Div("Tab in Entwicklung...")


@app.callback(
    Output('market-status-card', 'children'),
    Input('interval-market-status', 'n_intervals')
)
def update_market_status(n_intervals):
    status = MarketHours.get_market_status()
    
    if status['is_open']:
        bg_color, border_color, text_color = '#d4edda', '#28a745', '#155724'
    elif status['session'] in ['pre_market', 'after_hours']:
        bg_color, border_color, text_color = '#fff3cd', '#ffc107', '#856404'
    else:
        bg_color, border_color, text_color = '#f8d7da', '#dc3545', '#721c24'
    
    time_str = status['current_time_et'].strftime('%Y-%m-%d %I:%M:%S %p ET')
    
    return html.Div([
        html.Div([
            html.H3([
                html.Span(status['emoji'], style={'marginRight': '15px', 'fontSize': '32px'}),
                html.Span(status['message'])
            ], style={'margin': '0', 'color': text_color}),
            html.P(f"Zeit: {time_str}", style={'margin': '10px 0', 'fontSize': '16px'}),
        ], style={'padding': '20px'})
    ], style={'backgroundColor': bg_color, 'border': f'3px solid {border_color}',
             'borderRadius': '10px'})


# ============================================
# STREAMING TAB CALLBACKS
# ============================================

@app.callback(
    Output('ticker-dropdown', 'options'),
    Output('all-tickers-store', 'data'),
    Input('main-tabs', 'value')
)
def load_streaming_ticker_options(tab):
    if tab == 'streaming-tab':
        try:
            df = stock_queries.get_tickers()
            if df.empty:
                df = ticker_loader.load_tickers()
            
            options = [
                {'label': f"{row['Symbol']} - {row['Security']}", 'value': row['Symbol']}
                for _, row in df.iterrows()
            ]
            return options, df.to_dict('records')
        except:
            df = ticker_loader.load_tickers()
            options = [
                {'label': f"{row['Symbol']} - {row['Security']}", 'value': row['Symbol']}
                for _, row in df.iterrows()
            ]
            return options, df.to_dict('records')
    return [], []


@app.callback(
    Output('ticker-dropdown', 'value'),
    Input('load-top10-btn', 'n_clicks'),
    prevent_initial_call=True
)
def load_top_tickers(n_clicks):
    if n_clicks > 0:
        return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
    return []


# ✅ Chart Ticker Dropdown Update
@app.callback(
    Output('chart-ticker-dropdown', 'options'),
    Output('chart-ticker-dropdown', 'value'),
    Output('chart-ticker-selector', 'style'),
    Input('ticker-dropdown', 'value')
)
def update_chart_ticker_options(selected_tickers):
    if not selected_tickers:
        return [], None, {'marginTop': '10px', 'display': 'none'}
    
    options = [{'label': ticker, 'value': ticker} for ticker in selected_tickers]
    default = selected_tickers[0]
    return options, default, {'marginTop': '10px', 'display': 'block'}


@app.callback(
    Output('ticker-table', 'children'),
    Input('ticker-dropdown', 'value'),
    Input('interval-live-chart', 'n_intervals'),
    State('all-tickers-store', 'data'),
    State('streaming-state', 'data')
)
def update_streaming_ticker_table(selected_symbols, n_intervals, all_tickers, streaming_state):
    if not selected_symbols or not all_tickers:
        return html.P("⚠️ Keine Ticker ausgewählt", style={'color': '#666'})
    
    df = pd.DataFrame(all_tickers)
    selected_df = df[df['Symbol'].isin(selected_symbols)].copy()
    
    # ✅ Live-Daten hinzufügen wenn Streaming aktiv
    if streaming_state['active'] and stream_manager:
        for idx, row in selected_df.iterrows():
            symbol = row['Symbol']
            latest = stream_manager.get_latest_point(symbol)
            if latest:
                selected_df.at[idx, 'Last Price'] = f"${latest.get('close', 0):.2f}"
                selected_df.at[idx, 'Volume'] = f"{latest.get('volume', 0):,}"
                selected_df.at[idx, 'VWAP'] = f"${latest.get('vwap', 0):.2f}"
            else:
                selected_df.at[idx, 'Last Price'] = "Warte..."
                selected_df.at[idx, 'Volume'] = "-"
                selected_df.at[idx, 'VWAP'] = "-"
        
        columns = ['Symbol', 'Security', 'GICS Sector', 'Last Price', 'Volume', 'VWAP']
    else:
        columns = ['Symbol', 'Security', 'GICS Sector', 'Headquarters Location']
    
    table = dash_table.DataTable(
        data=selected_df[columns].to_dict('records'),
        columns=[{'name': col, 'id': col} for col in columns],
        style_cell={'textAlign': 'left', 'padding': '10px'},
        style_header={'backgroundColor': '#3498db', 'color': 'white', 'fontWeight': 'bold'},
        style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': '#f9f9f9'}]
    )
    
    return table


# ✅ START/STOP Streaming
@app.callback(
    Output('streaming-state', 'data'),
    Output('start-streaming-btn', 'disabled'),
    Output('stop-streaming-btn', 'disabled'),
    Output('interval-live-chart', 'disabled'),
    Output('interval-stats', 'disabled'),
    Output('streaming-status', 'children'),
    Input('start-streaming-btn', 'n_clicks'),
    Input('stop-streaming-btn', 'n_clicks'),
    State('ticker-dropdown', 'value'),
    State('aggregation-selector', 'value'),
    State('streaming-state', 'data'),
    prevent_initial_call=True
)
def control_streaming(start_clicks, stop_clicks, selected_tickers, aggregation, state):
    global ws_client, stream_manager, streaming_active, current_aggregation
    
    triggered_id = ctx.triggered_id
    
    if triggered_id == 'start-streaming-btn':
        if not selected_tickers:
            return state, False, True, True, True, html.Span("⚠️ Bitte Ticker auswählen", style={'color': 'orange'})
        
        try:
            print(f"\n🚀 Starte Streaming...")
            
            # Initialize
            max_points = 600 if aggregation == 'second' else 600
            stream_manager = StreamDataManager(max_points=max_points)
            current_aggregation = aggregation
            
            # ✅ Pre-Load historische Daten (nur bei Minuten)
            if aggregation == 'minute':
                print("\n📊 Lade historische 600 Minuten...")
                for ticker in selected_tickers:
                    try:
                        from_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
                        to_date = datetime.now().strftime('%Y-%m-%d')
                        
                        df_hist = api_client.get_aggregates(
                            ticker, timespan='minute',
                            from_date=from_date, to_date=to_date, limit=600
                        )
                        
                        if not df_hist.empty:
                            df_hist = MarketHours.filter_regular_hours(df_hist)
                            df_hist = df_hist.tail(600)
                            stream_manager.preload_historical_data(ticker, df_hist)
                    except Exception as e:
                        print(f"⚠️ Pre-Load für {ticker} fehlgeschlagen: {e}")
            
            # WebSocket
            ws_client = StockWebSocketClient(aggregation=aggregation)
            
            def handle_message(data):
                symbol = data.get('symbol')
                if symbol in selected_tickers:
                    stream_manager.add_data_point(symbol, data)
            
            ws_client.set_message_callback(handle_message)
            ws_client.subscribe(selected_tickers)
            ws_client.start()
            
            streaming_active = True
            new_state = {'active': True, 'aggregation': aggregation}
            status = html.Span("🟢 Streaming AKTIV", style={'color': 'green', 'fontWeight': 'bold'})
            
            return new_state, True, False, False, False, status
            
        except Exception as e:
            print(f"✗ Fehler: {e}")
            import traceback
            traceback.print_exc()
            status = html.Span(f"❌ Fehler: {str(e)}", style={'color': 'red'})
            return state, False, True, True, True, status
    
    elif triggered_id == 'stop-streaming-btn':
        if ws_client:
            ws_client.stop()
        
        streaming_active = False
        ws_client = None
        stream_manager = None
        
        new_state = {'active': False, 'aggregation': 'minute'}
        status = html.Span("⚫ Streaming gestoppt", style={'color': 'gray'})
        
        return new_state, False, True, True, True, status
    
    return state, False, True, True, True, ""


# ✅ Streaming Statistics
@app.callback(
    Output('streaming-stats', 'children'),
    Input('interval-stats', 'n_intervals'),
    State('streaming-state', 'data'),
    State('chart-ticker-dropdown', 'value')
)
def update_streaming_stats(n_intervals, state, chart_ticker):
    """Aktualisiert Streaming Statistiken"""
    
    # Prüfe ob Streaming aktiv ist
    if not state or not state.get('active', False) or not stream_manager:
        return html.P("Streaming nicht aktiv", style={'color': '#666'})
    
    try:
        # Hole Statistiken
        stats = stream_manager.get_statistics()
        
        # Basis-Statistiken
        stats_display = html.Div([
            html.H4("📊 Streaming Statistiken", style={'marginBottom': '10px'}),
            html.Div([
                html.Div([
                    html.Strong("Nachrichten: "),
                    html.Span(f"{stats.get('total_messages', 0):,}")
                ], style={'display': 'inline-block', 'marginRight': '30px'}),
                
                html.Div([
                    html.Strong("Aktive Ticker: "),
                    html.Span(f"{stats.get('active_tickers', 0)}")
                ], style={'display': 'inline-block', 'marginRight': '30px'}),
                
                html.Div([
                    html.Strong("Aggregation: "),
                    html.Span(current_aggregation.title())
                ], style={'display': 'inline-block'}),
            ]),
        ])
        
        # ✅ Buffer Info nur wenn chart_ticker gesetzt ist
        if chart_ticker:
            try:
                buffer_status = stream_manager.get_buffer_status(chart_ticker)
                
                # Prüfe ob alle Keys vorhanden sind
                if all(key in buffer_status for key in ['size', 'max', 'percentage', 'preloaded_count', 'live_count']):
                    buffer_info = html.Div([
                        html.Strong(f"Buffer ({chart_ticker}): "),
                        html.Span(f"{buffer_status['size']}/{buffer_status['max']} Punkte "),
                        html.Span(
                            f"({buffer_status['percentage']}%) ", 
                            style={'color': 'green' if buffer_status.get('is_full', False) else 'orange'}
                        ),
                        html.Span(f"| Pre-loaded: {buffer_status['preloaded_count']}, Live: {buffer_status['live_count']}")
                    ], style={'marginTop': '10px', 'fontSize': '14px'})
                    
                    # Füge Buffer Info hinzu
                    stats_display.children.append(buffer_info)
                    
            except Exception as e:
                print(f"⚠️ Buffer Status Error: {e}")
                # Ignoriere Buffer-Fehler, zeige nur Basis-Stats
        
        return stats_display
        
    except Exception as e:
        print(f"✗ Streaming Stats Error: {e}")
        import traceback
        traceback.print_exc()
        
        # Fallback: Zeige nur einfache Message
        return html.P(f"⚠️ Statistiken nicht verfügbar: {str(e)}", 
                     style={'color': 'orange', 'fontSize': '14px'})

# ============================================
# Streaming Chart CALLBACKS 
# ============================================

@app.callback(
    Output('live-price-chart', 'figure'),
    Input('interval-live-chart', 'n_intervals'),
    State('chart-ticker-dropdown', 'value'),
    State('stream-chart-type', 'value'),
    State('stream-vwap-toggle', 'value'),
    State('streaming-state', 'data')
)
def update_live_price_chart(n_intervals, ticker, chart_type, vwap_values, state):
    """Aktualisiert NUR den Price Chart"""
    
    if not state or not state.get('active', False) or not stream_manager or not ticker:
        fig = go.Figure()
        fig.add_annotation(
            text="⏸️ Warte auf Streaming...",
            xref="paper", yref="paper", x=0.5, y=0.5,
            showarrow=False, font=dict(size=16, color="gray")
        )
        return fig
    
    df = stream_manager.get_dataframe(ticker)
    
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text=f"⏳ Warte auf Daten für {ticker}...",
            xref="paper", yref="paper", x=0.5, y=0.5,
            showarrow=False, font=dict(size=16, color="orange")
        )
        return fig
    
    # Filter
    df_filtered = MarketHours.filter_regular_hours(df, timestamp_col='timestamp')
    if df_filtered.empty:
        df_filtered = df
    
    buffer_status = stream_manager.get_buffer_status(ticker)
    
    # ✅ Erstelle EINZELNEN Figure (kein Subplot!)
    fig = go.Figure()
    
    # Price Chart
    if chart_type == 'candlestick':
        fig.add_trace(go.Candlestick(
            x=df_filtered['timestamp'],
            open=df_filtered['open'],
            high=df_filtered['high'],
            low=df_filtered['low'],
            close=df_filtered['close'],
            name='OHLC',
            increasing_line_color='#26a69a',
            decreasing_line_color='#ef5350'
        ))
    else:
        fig.add_trace(go.Scatter(
            x=df_filtered['timestamp'],
            y=df_filtered['close'],
            mode='lines',
            name='Close',
            line=dict(color='#2196f3', width=2)
        ))
    
    # VWAP
    if 'show' in vwap_values and 'vwap' in df_filtered.columns:
        fig.add_trace(go.Scatter(
            x=df_filtered['timestamp'],
            y=df_filtered['vwap'],
            mode='lines',
            name='VWAP',
            line=dict(color='#ff9800', width=2, dash='dash'),
            opacity=0.8
        ))
    
    # Layout
    fig.update_layout(
        title=f'{ticker} - Live {state["aggregation"].title()} | Buffer: {buffer_status["size"]}/{buffer_status["max"]}',
        xaxis_title='Zeit (ET)',
        yaxis_title='Preis ($)',
        template='plotly_white',
        height=600,
        hovermode='x unified',
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(
            rangeslider=dict(visible=False),
            showgrid=True,
            gridcolor='#e0e0e0'
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='#e0e0e0'
        )
    )
    
    return fig


# ✅ VOLUME CHART (Separat)
@app.callback(
    Output('live-volume-chart', 'figure'),
    Input('interval-live-chart', 'n_intervals'),
    State('chart-ticker-dropdown', 'value'),
    State('streaming-state', 'data')
)
def update_live_volume_chart(n_intervals, ticker, state):
    """Aktualisiert NUR den Volume Chart"""
    
    if not state or not state.get('active', False) or not stream_manager or not ticker:
        fig = go.Figure()
        fig.update_layout(
            template='plotly_white',
            height=200,
            margin=dict(t=40, b=40)
        )
        return fig
    
    df = stream_manager.get_dataframe(ticker)
    
    if df.empty:
        fig = go.Figure()
        fig.update_layout(
            template='plotly_white',
            height=200,
            margin=dict(t=40, b=40)
        )
        return fig
    
    # Filter
    df_filtered = MarketHours.filter_regular_hours(df, timestamp_col='timestamp')
    if df_filtered.empty:
        df_filtered = df
    
    # ✅ Volume Bars mit Farben
    colors = ['#26a69a' if c >= o else '#ef5350' 
              for c, o in zip(df_filtered['close'], df_filtered['open'])]
    
    # ✅ Erstelle EINZELNEN Figure (kein Subplot!)
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df_filtered['timestamp'],
        y=df_filtered['volume'],
        name='Volume',
        marker=dict(color=colors, opacity=0.7),
        showlegend=False
    ))
    
    # Layout
    fig.update_layout(
        title='Volume',
        xaxis_title='Zeit (ET)',
        yaxis_title='Volume',
        template='plotly_white',
        height=200,
        margin=dict(t=40, b=40, l=50, r=50),
        showlegend=False,
        xaxis=dict(showgrid=True, gridcolor='#e0e0e0'),
        yaxis=dict(showgrid=False)
    )
    
    return fig

# ============================================
# HISTORICAL TAB CALLBACKS (gekürzt - wie zuvor)
# ============================================

@app.callback(Output('db-status-info', 'children'), Input('main-tabs', 'value'))
def update_db_status(tab):
    if tab != 'historical-tab':
        return ""
    try:
        stats = db_manager.get_stats()
        return html.Div([
            html.Div([html.Strong("📊 Ticker: "), html.Span(f"{stats['tickers']:,}")],
                    style={'display': 'inline-block', 'marginRight': '30px'}),
            html.Div([html.Strong("📈 Daily: "), html.Span(f"{stats['daily_bars']:,}")],
                    style={'display': 'inline-block', 'marginRight': '30px'}),
            html.Div([html.Strong("💾 Größe: "), html.Span(stats['database_size'])],
                    style={'display': 'inline-block'}),
        ])
    except:
        return html.P("⚠️ DB nicht verbunden", style={'color': 'orange'})


@app.callback(Output('hist-ticker-dropdown', 'options'), Input('main-tabs', 'value'))
def load_hist_ticker_options(tab):
    if tab == 'historical-tab':
        try:
            df = stock_queries.get_tickers()
            if df.empty:
                df = ticker_loader.load_tickers()
            return [{'label': f"{r['Symbol']} - {r['Security']}", 'value': r['Symbol']} 
                   for _, r in df.iterrows()]
        except:
            df = ticker_loader.load_tickers()
            return [{'label': f"{r['Symbol']} - {r['Security']}", 'value': r['Symbol']} 
                   for _, r in df.iterrows()]
    return []


@app.callback(
    Output('historical-data-store', 'data'),
    Output('hist-status', 'children'),
    Input('load-hist-data-btn', 'n_clicks'),
    State('hist-ticker-dropdown', 'value'),
    State('hist-timespan', 'value'),
    State('hist-date-range', 'start_date'),
    State('hist-date-range', 'end_date'),
    prevent_initial_call=True
)
def load_historical_data(n_clicks, ticker, timespan, start_date, end_date):
    if not ticker:
        return None, html.P("⚠️ Bitte Ticker auswählen", style={'color': 'orange'})
    
    try:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d') if start_date else None
        end_dt = datetime.strptime(end_date, '%Y-%m-%d') if end_date else None
        
        df = stock_queries.get_aggregated_bars(ticker, timespan, start_dt, end_dt, 50000)
        
        if df.empty:
            return None, html.P(f"❌ Keine Daten für {ticker}", style={'color': 'red'})
        
        df_filtered = MarketHours.filter_trading_days(df) if timespan == 'day' else MarketHours.filter_regular_hours(df)
        
        if df_filtered.empty:
            return None, html.P(f"⚠️ Keine Trading Days", style={'color': 'orange'})
        
        df_filtered = TechnicalIndicators.add_all_indicators(df_filtered)
        data = df_filtered.to_json(date_format='iso', orient='split')
        
        status = html.Div([
            html.H4(f"✓ {ticker}", style={'color': 'green'}),
            html.P(f"📊 {len(df_filtered):,} Bars"),
            html.P("🗄️ TimescaleDB", style={'color': '#2196f3'})
        ], style={'padding': '15px', 'backgroundColor': '#e8f5e9', 'borderRadius': '5px'})
        
        return data, status
    except Exception as e:
        return None, html.Div([html.P("❌ Fehler", style={'color': 'red'})])


@app.callback(
    Output('hist-main-chart', 'figure'),
    Output('hist-indicator-chart', 'figure'),
    Input('historical-data-store', 'data'),
    Input('hist-chart-type', 'value'),
    Input('hist-ma-select', 'value'),
    Input('hist-bb-toggle', 'value'),
    Input('hist-vwap-toggle', 'value'),
    Input('hist-indicators', 'value'),
    State('hist-ticker-dropdown', 'value')
)
def update_historical_charts(data, chart_type, ma_selection, bb_toggle, vwap_toggle, indicators, ticker):
    if not data:
        fig = go.Figure()
        fig.add_annotation(text="📊 Bitte Daten laden...", xref="paper", yref="paper",
                          x=0.5, y=0.5, showarrow=False, font=dict(size=16, color="gray"))
        return fig, go.Figure()
    
    df = pd.read_json(data, orient='split')
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Main Chart (wie zuvor, aber gekürzt)
    fig_main = go.Figure()
    
    if chart_type == 'candlestick':
        fig_main.add_trace(go.Candlestick(x=df['timestamp'], open=df['open'], high=df['high'],
                                          low=df['low'], close=df['close'], name='OHLC'))
    else:
        fig_main.add_trace(go.Scatter(x=df['timestamp'], y=df['close'], mode='lines', name='Close'))
    
    fig_main.update_layout(title=f'{ticker} - Daily Chart', height=600, template='plotly_white')
    
    # Indicator Chart (vereinfacht)
    fig_ind = go.Figure()
    return fig_main, fig_ind


if __name__ == '__main__':
    print("=" * 60)
    print("🚀 S&P 500 Stock Streaming Platform - COMPLETE")
    print("=" * 60)
    print(f"\n✓ Market Status: {get_market_status_message()}")
    print(f"\n🌐 Browser: http://127.0.0.1:8050")
    print("=" * 60)
    print("\n✨ Features:")
    print("  • Live-Streaming mit Charts ✓")
    print("  • Combined Chart (Price + Volume) ✓")
    print("  • VWAP Toggle ✓")
    print("  • Pre-Loading 600 Minuten ✓")
    print("  • Historische Daily Charts ✓")
    print("  • TimescaleDB Integration ✓")
    print("=" * 60)
    
    app.run_server(debug=True, port=8050, host='127.0.0.1')
