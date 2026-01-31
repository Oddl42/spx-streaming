#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 31 16:58:23 2026

@author: twi-dev
"""

# src/visualization/app.py
"""
S&P 500 Stock Streaming Platform - PHASE 7 COMPLETE
Mit Historischem Daten Tab & Technischen Indikatoren
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
from src.streaming.stream_manager import StreamDataManager
from src.utils.indicators import TechnicalIndicators

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
    html.H1("📊 S&P 500 Stock Streaming Platform - Phase 7", 
            style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': '20px'}),
    
    dcc.Tabs(id='main-tabs', value='streaming-tab', children=[
        dcc.Tab(label='🔴 Streaming (Live)', value='streaming-tab'),
        dcc.Tab(label='📈 Historische Daten', value='historical-tab'),  # ✅ Aktiviert!
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
# Streaming Tab Layout (gekürzt - aus Phase 6)
# ============================================

def create_streaming_tab():
    return html.Div([
        html.H2("🎯 Real-time Stock Streaming"),
        html.Div(id='market-status-card', style={'marginBottom': '30px'}),
        
        # ... (Rest wie in Phase 6) ...
        html.P("Streaming Tab - Siehe Phase 6 Code", style={'color': '#666'}),
        
        # Stores
        dcc.Store(id='all-tickers-store'),
    ])


# ============================================
# HISTORICAL DATA TAB LAYOUT (NEU!)
# ============================================

def create_historical_tab():
    return html.Div([
        html.H2("📈 Historische Daten & Technische Analyse"),
        
        # Controls Row
        html.Div([
            # Left Column: Ticker & Timeframe
            html.Div([
                html.H3("🎯 Daten-Auswahl"),
                
                # Ticker Selection
                html.Label("Ticker:", style={'fontWeight': 'bold', 'marginTop': '10px'}),
                dcc.Dropdown(
                    id='hist-ticker-dropdown',
                    options=[],
                    multi=False,
                    placeholder='Ticker auswählen...',
                ),
                
                # Timespan
                html.Label("Timespan:", style={'fontWeight': 'bold', 'marginTop': '15px'}),
                dcc.RadioItems(
                    id='hist-timespan',
                    options=[
                        {'label': ' 1 Minute', 'value': 'minute'},
                        {'label': ' 5 Minuten', 'value': '5minute'},
                        {'label': ' 15 Minuten', 'value': '15minute'},
                        {'label': ' 1 Stunde', 'value': 'hour'},
                        {'label': ' 1 Tag', 'value': 'day'}
                    ],
                    value='hour',
                    style={'marginBottom': '15px'}
                ),
                
                # Date Range
                html.Label("Zeitraum:", style={'fontWeight': 'bold', 'marginTop': '15px'}),
                dcc.DatePickerRange(
                    id='hist-date-range',
                    start_date=(datetime.now() - timedelta(days=30)).date(),
                    end_date=datetime.now().date(),
                    display_format='YYYY-MM-DD',
                    style={'marginBottom': '15px'}
                ),
                
                # Load Button
                html.Button('📊 Daten laden', id='load-hist-data-btn', n_clicks=0,
                           style={'width': '100%', 'padding': '10px', 'marginTop': '15px',
                                 'backgroundColor': '#2196f3', 'color': 'white', 
                                 'border': 'none', 'borderRadius': '5px',
                                 'cursor': 'pointer', 'fontSize': '16px'}),
                
            ], style={'width': '30%', 'display': 'inline-block', 'verticalAlign': 'top',
                     'padding': '20px', 'backgroundColor': '#f8f9fa', 'borderRadius': '8px'}),
            
            # Right Column: Chart Settings & Indicators
            html.Div([
                html.H3("📐 Chart & Indikatoren"),
                
                # Chart Type
                html.Label("Chart Type:", style={'fontWeight': 'bold'}),
                dcc.RadioItems(
                    id='hist-chart-type',
                    options=[
                        {'label': ' Candlestick', 'value': 'candlestick'},
                        {'label': ' Line Chart', 'value': 'line'}
                    ],
                    value='candlestick',
                    inline=True,
                    style={'marginBottom': '15px'}
                ),
                
                # Moving Averages
                html.Label("Moving Averages:", style={'fontWeight': 'bold'}),
                dcc.Checklist(
                    id='hist-ma-select',
                    options=[
                        {'label': ' SMA 20', 'value': 'SMA_20'},
                        {'label': ' SMA 50', 'value': 'SMA_50'},
                        {'label': ' SMA 200', 'value': 'SMA_200'},
                        {'label': ' EMA 12', 'value': 'EMA_12'},
                        {'label': ' EMA 26', 'value': 'EMA_26'}
                    ],
                    value=['SMA_20', 'SMA_50'],
                    inline=True,
                    style={'marginBottom': '15px'}
                ),
                
                # Bollinger Bands
                html.Div([
                    dcc.Checklist(
                        id='hist-bb-toggle',
                        options=[{'label': ' Bollinger Bands (20, 2σ)', 'value': 'show'}],
                        value=[],
                        inline=True
                    )
                ], style={'marginBottom': '15px'}),
                
                # VWAP
                html.Div([
                    dcc.Checklist(
                        id='hist-vwap-toggle',
                        options=[{'label': ' VWAP', 'value': 'show'}],
                        value=[],
                        inline=True
                    )
                ], style={'marginBottom': '15px'}),
                
                # Additional Indicators
                html.Label("Zusätzliche Indikatoren:", style={'fontWeight': 'bold', 'marginTop': '10px'}),
                dcc.Checklist(
                    id='hist-indicators',
                    options=[
                        {'label': ' RSI (14)', 'value': 'RSI'},
                        {'label': ' MACD', 'value': 'MACD'},
                        {'label': ' Volume', 'value': 'Volume'}
                    ],
                    value=['Volume'],
                    inline=True
                ),
                
            ], style={'width': '65%', 'display': 'inline-block', 'verticalAlign': 'top',
                     'marginLeft': '5%', 'padding': '20px', 'backgroundColor': '#f8f9fa',
                     'borderRadius': '8px'}),
        ]),
        
        # Status & Info
        html.Div(id='hist-status', style={'marginTop': '20px', 'marginBottom': '20px',
                                          'padding': '15px', 'backgroundColor': '#e3f2fd',
                                          'borderRadius': '5px'}),
        
        # Charts
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


# Load Ticker Options (für beide Tabs)
@app.callback(
    Output('hist-ticker-dropdown', 'options'),
    Input('main-tabs', 'value')
)
def load_hist_ticker_options(tab):
    if tab == 'historical-tab':
        df = ticker_loader.load_tickers()
        options = [
            {'label': f"{row['Symbol']} - {row['Security']}", 'value': row['Symbol']}
            for _, row in df.iterrows()
        ]
        return options
    return []


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
# HISTORICAL DATA CALLBACKS (NEU!)
# ============================================

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
    """Lädt historische Daten"""
    
    if not ticker:
        return None, html.P("⚠️ Bitte Ticker auswählen", style={'color': 'orange'})
    
    try:
        print(f"\n📊 Lade historische Daten für {ticker}...")
        print(f"   Timespan: {timespan}")
        print(f"   Zeitraum: {start_date} bis {end_date}")
        
        # Timespan Mapping
        timespan_map = {
            'minute': ('minute', 1),
            '5minute': ('minute', 5),
            '15minute': ('minute', 15),
            'hour': ('hour', 1),
            'day': ('day', 1)
        }
        
        ts_type, multiplier = timespan_map.get(timespan, ('hour', 1))
        
        # API Call
        df = api_client.get_aggregates(
            ticker=ticker,
            multiplier=multiplier,
            timespan=ts_type,
            from_date=start_date,
            to_date=end_date,
            limit=50000
        )
        
        if df.empty:
            return None, html.P(f"❌ Keine Daten für {ticker} verfügbar", 
                              style={'color': 'red'})
        
        # Filter Market Hours
        df_filtered = MarketHours.filter_regular_hours(df)
        
        if df_filtered.empty:
            df_filtered = df  # Falls keine Market Hours Daten
        
        # Indikatoren berechnen
        print("📐 Berechne technische Indikatoren...")
        df_filtered = TechnicalIndicators.add_all_indicators(df_filtered)
        
        # Store als JSON
        data = df_filtered.to_json(date_format='iso', orient='split')
        
        status = html.Div([
            html.H4(f"✓ Daten geladen für {ticker}", style={'color': 'green'}),
            html.P(f"Datenpunkte: {len(df_filtered):,}"),
            html.P(f"Zeitraum: {df_filtered['timestamp'].min()} bis {df_filtered['timestamp'].max()}"),
            html.P(f"Timespan: {timespan} | Indikatoren: ✓ Berechnet")
        ])
        
        return data, status
        
    except Exception as e:
        print(f"✗ Fehler: {e}")
        import traceback
        traceback.print_exc()
        return None, html.P(f"❌ Fehler: {str(e)}", style={'color': 'red'})


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
def update_historical_charts(data, chart_type, ma_selection, bb_toggle, vwap_toggle, 
                             indicators, ticker):
    """Aktualisiert historische Charts"""
    
    if not data or not ticker:
        # Empty charts
        fig_main = go.Figure()
        fig_main.add_annotation(text="📊 Bitte Daten laden...",
                               xref="paper", yref="paper", x=0.5, y=0.5,
                               showarrow=False, font=dict(size=16, color="gray"))
        
        fig_indicators = go.Figure()
        return fig_main, fig_indicators
    
    # Load Data
    df = pd.read_json(data, orient='split')
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # ========================================
    # MAIN CHART
    # ========================================
    
    fig_main = go.Figure()
    
    # Price Chart (Candlestick or Line)
    if chart_type == 'candlestick':
        fig_main.add_trace(go.Candlestick(
            x=df['timestamp'],
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close'],
            name='OHLC',
            increasing_line_color='#26a69a',
            decreasing_line_color='#ef5350'
        ))
    else:
        fig_main.add_trace(go.Scatter(
            x=df['timestamp'],
            y=df['close'],
            mode='lines',
            name='Close',
            line=dict(color='#2196f3', width=2)
        ))
    
    # Moving Averages
    ma_colors = {
        'SMA_20': '#ff6b6b',
        'SMA_50': '#4ecdc4',
        'SMA_200': '#45b7d1',
        'EMA_12': '#feca57',
        'EMA_26': '#ff9ff3'
    }
    
    for ma in ma_selection:
        if ma in df.columns:
            fig_main.add_trace(go.Scatter(
                x=df['timestamp'],
                y=df[ma],
                mode='lines',
                name=ma,
                line=dict(color=ma_colors.get(ma, '#888'), width=2, dash='dash'),
                opacity=0.7
            ))
    
    # Bollinger Bands
    if 'show' in bb_toggle and 'BB_Upper' in df.columns:
        fig_main.add_trace(go.Scatter(
            x=df['timestamp'],
            y=df['BB_Upper'],
            mode='lines',
            name='BB Upper',
            line=dict(color='rgba(128,128,128,0.5)', width=1),
            showlegend=False
        ))
        
        fig_main.add_trace(go.Scatter(
            x=df['timestamp'],
            y=df['BB_Lower'],
            mode='lines',
            name='BB Lower',
            line=dict(color='rgba(128,128,128,0.5)', width=1),
            fill='tonexty',
            fillcolor='rgba(128,128,128,0.1)',
            showlegend=False
        ))
        
        fig_main.add_trace(go.Scatter(
            x=df['timestamp'],
            y=df['BB_Middle'],
            mode='lines',
            name='BB Middle',
            line=dict(color='gray', width=1, dash='dot'),
            opacity=0.5
        ))
    
    # VWAP
    if 'show' in vwap_toggle and 'vwap' in df.columns:
        fig_main.add_trace(go.Scatter(
            x=df['timestamp'],
            y=df['vwap'],
            mode='lines',
            name='VWAP',
            line=dict(color='#ff9800', width=2, dash='dash')
        ))
    
    # Main Chart Layout
    fig_main.update_layout(
        title=f'{ticker} - Historische Daten mit Technischer Analyse',
        xaxis_title='Datum',
        yaxis_title='Preis ($)',
        template='plotly_white',
        height=600,
        hovermode='x unified',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(rangeslider=dict(visible=False))
    )
    
    # ========================================
    # INDICATOR CHART (RSI, MACD, Volume)
    # ========================================
    
    # Count active indicators
    active_indicators = [ind for ind in indicators if ind in ['RSI', 'MACD', 'Volume']]
    n_indicators = len(active_indicators)
    
    if n_indicators == 0:
        fig_indicators = go.Figure()
        fig_indicators.add_annotation(text="Keine Indikatoren ausgewählt",
                                     xref="paper", yref="paper", x=0.5, y=0.5,
                                     showarrow=False, font=dict(size=14, color="gray"))
        return fig_main, fig_indicators
    
    # Create Subplots
    fig_indicators = make_subplots(
        rows=n_indicators, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=active_indicators
    )
    
    row = 1
    
    # RSI
    if 'RSI' in active_indicators and 'RSI' in df.columns:
        fig_indicators.add_trace(
            go.Scatter(x=df['timestamp'], y=df['RSI'], mode='lines', 
                      name='RSI', line=dict(color='#9b59b6', width=2)),
            row=row, col=1
        )
        # RSI Reference Lines
        fig_indicators.add_hline(y=70, line_dash="dash", line_color="red", 
                                opacity=0.5, row=row, col=1)
        fig_indicators.add_hline(y=30, line_dash="dash", line_color="green", 
                                opacity=0.5, row=row, col=1)
        fig_indicators.update_yaxes(title_text="RSI", row=row, col=1, range=[0, 100])
        row += 1
    
    # MACD
    if 'MACD' in active_indicators and 'MACD' in df.columns:
        fig_indicators.add_trace(
            go.Scatter(x=df['timestamp'], y=df['MACD'], mode='lines',
                      name='MACD', line=dict(color='#3498db', width=2)),
            row=row, col=1
        )
        fig_indicators.add_trace(
            go.Scatter(x=df['timestamp'], y=df['MACD_Signal'], mode='lines',
                      name='Signal', line=dict(color='#e74c3c', width=2)),
            row=row, col=1
        )
        fig_indicators.add_trace(
            go.Bar(x=df['timestamp'], y=df['MACD_Hist'], name='Histogram',
                  marker=dict(color=df['MACD_Hist'].apply(
                      lambda x: '#26a69a' if x > 0 else '#ef5350'
                  ))),
            row=row, col=1
        )
        fig_indicators.update_yaxes(title_text="MACD", row=row, col=1)
        row += 1
    
    # Volume
    if 'Volume' in active_indicators and 'volume' in df.columns:
        colors = ['#26a69a' if c >= o else '#ef5350' 
                 for c, o in zip(df['close'], df['open'])]
        
        fig_indicators.add_trace(
            go.Bar(x=df['timestamp'], y=df['volume'], name='Volume',
                  marker=dict(color=colors, opacity=0.6)),
            row=row, col=1
        )
        fig_indicators.update_yaxes(title_text="Volume", row=row, col=1)
    
    # Indicator Chart Layout
    fig_indicators.update_layout(
        height=300,
        template='plotly_white',
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=50, b=50)
    )
    
    fig_indicators.update_xaxes(title_text="Datum", row=n_indicators, col=1)
    
    return fig_main, fig_indicators


# ============================================
# Run Server
# ============================================

if __name__ == '__main__':
    print("=" * 60)
    print("🚀 S&P 500 Stock Streaming Platform - PHASE 7")
    print("=" * 60)
    print(f"\n✓ Market Status: {get_market_status_message()}")
    print(f"\n🌐 Browser: http://127.0.0.1:8050")
    print("=" * 60)
    print("\n✨ Phase 7 Features:")
    print("  • Historische Daten Tab")
    print("  • Date Range Picker")
    print("  • Multiple Timeframes (1m, 5m, 15m, 1h, 1d)")
    print("  • Moving Averages (SMA, EMA)")
    print("  • Bollinger Bands")
    print("  • RSI Indikator")
    print("  • MACD Indikator")
    print("  • Volume Chart")
    print("  • VWAP")
    print("=" * 60)
    
    app.run_server(debug=True, port=8050, host='127.0.0.1')
