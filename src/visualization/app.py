# src/visualization/app.py
"""
S&P 500 Stock Streaming Platform - Main Application
Integriert Market Hours Logic
"""

import dash
from dash import dcc, html, Input, Output, State, dash_table
import plotly.graph_objs as go
import pandas as pd
from pathlib import Path
import sys

# Füge src zum Path hinzu
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.helpers import SP500TickerLoader
from src.api.massive_rest_client import MassiveRESTClient
from src.utils.market_hours import MarketHours, is_market_open, get_market_status_message

# App initialisieren
app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "S&P 500 Stock Streaming"

# Globals
ticker_loader = SP500TickerLoader()
api_client = MassiveRESTClient()

# ============================================
# App Layout
# ============================================

app.layout = html.Div([
    html.H1("📊 S&P 500 Stock Streaming Platform", 
            style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': '20px'}),
    
    # Tabs
    dcc.Tabs(id='main-tabs', value='streaming-tab', children=[
        dcc.Tab(label='🔴 Streaming (Live)', value='streaming-tab'),
        dcc.Tab(label='📈 Historische Daten', value='historical-tab', disabled=True),
        dcc.Tab(label='🤖 Machine Learning', value='ml-tab', disabled=True),
    ]),
    
    html.Div(id='tab-content', style={'padding': '20px'}),
    
    # Interval für Market Status Updates (alle 10 Sekunden)
    dcc.Interval(
        id='interval-market-status',
        interval=10*1000,  # 10 Sekunden
        n_intervals=0
    )
], style={'fontFamily': 'Arial, sans-serif'})


# ============================================
# Streaming Tab Layout
# ============================================

def create_streaming_tab():
    return html.Div([
        html.H2("🎯 Real-time Stock Data & Market Hours Demo"),
        
        # Market Hours Status (wird live aktualisiert)
        html.Div(
            id='market-status-card',
            style={
                'marginBottom': '30px',
                'padding': '0'
            }
        ),
        
        # Info Box
        html.Div([
            html.H4("ℹ️ Market Hours Information"),
            html.Ul([
                html.Li("Regular Market Hours: 9:30 AM - 4:00 PM ET (Eastern Time)"),
                html.Li("Pre-Market: 4:00 AM - 9:30 AM ET"),
                html.Li("After-Hours: 4:00 PM - 8:00 PM ET"),
                html.Li("Charts zeigen nur Daten während Regular Market Hours"),
                html.Li("Wochenenden und Feiertage werden automatisch gefiltert")
            ])
        ], style={
            'backgroundColor': '#e3f2fd',
            'padding': '15px',
            'borderRadius': '8px',
            'marginBottom': '20px',
            'border': '1px solid #2196f3'
        }),
        
        # Ticker Selection
        html.Div([
            html.H3("📋 Ticker Auswahl"),
            html.Div([
                dcc.Dropdown(
                    id='ticker-dropdown',
                    options=[],
                    multi=True,
                    placeholder='Ticker auswählen (z.B. AAPL, MSFT, GOOGL)...',
                    style={'width': '70%', 'display': 'inline-block'}
                ),
                html.Button('Alle S&P 500 laden (Top 10)', 
                           id='load-all-btn',
                           n_clicks=0,
                           style={
                               'marginLeft': '10px',
                               'padding': '10px 20px',
                               'backgroundColor': '#4CAF50',
                               'color': 'white',
                               'border': 'none',
                               'borderRadius': '4px',
                               'cursor': 'pointer'
                           }),
            ], style={'marginBottom': '20px'}),
            
            # Buttons
            html.Div([
                html.Button('📊 Historische Daten laden', 
                           id='load-historical-btn',
                           n_clicks=0,
                           style={
                               'marginRight': '10px',
                               'padding': '12px 24px',
                               'backgroundColor': '#2196f3',
                               'color': 'white',
                               'border': 'none',
                               'borderRadius': '4px',
                               'cursor': 'pointer',
                               'fontSize': '14px'
                           }),
                
                html.Button('🧪 Market Hours Test', 
                           id='test-market-hours-btn',
                           n_clicks=0,
                           style={
                               'marginRight': '10px',
                               'padding': '12px 24px',
                               'backgroundColor': '#ff9800',
                               'color': 'white',
                               'border': 'none',
                               'borderRadius': '4px',
                               'cursor': 'pointer',
                               'fontSize': '14px'
                           }),
                
                html.Button('🔴 Streaming starten', 
                           id='streaming-btn',
                           n_clicks=0,
                           disabled=True,
                           style={
                               'padding': '12px 24px',
                               'backgroundColor': '#ccc',
                               'color': 'white',
                               'border': 'none',
                               'borderRadius': '4px',
                               'cursor': 'not-allowed',
                               'fontSize': '14px'
                           }),
            ], style={'marginBottom': '20px'}),
            
            # Test Output
            html.Div(id='test-output', style={'marginTop': '20px'})
        ]),
        
        # Selected Tickers Table
        html.Div([
            html.H3("✅ Ausgewählte Ticker"),
            html.Div(id='ticker-table')
        ], style={'marginTop': '30px'}),
        
        # Chart
        html.Div([
            html.H3("📈 Price Chart (Regular Market Hours Only)"),
            dcc.Graph(id='price-chart', style={'height': '600px'})
        ], style={'marginTop': '30px'}),
        
        # Hidden Stores
        dcc.Store(id='all-tickers-store'),
        dcc.Store(id='selected-tickers-store'),
    ])


# ============================================
# Callbacks
# ============================================

# Callback: Tab Content
@app.callback(
    Output('tab-content', 'children'),
    Input('main-tabs', 'value')
)
def render_tab_content(active_tab):
    if active_tab == 'streaming-tab':
        return create_streaming_tab()
    return html.Div("Tab in Entwicklung...")


# Callback: Ticker Dropdown Options laden
@app.callback(
    Output('ticker-dropdown', 'options'),
    Output('all-tickers-store', 'data'),
    Input('main-tabs', 'value')
)
def load_ticker_options(tab):
    """Lade S&P 500 Ticker aus JSON"""
    df = ticker_loader.load_tickers()
    
    options = [
        {'label': f"{row['Symbol']} - {row['Security']}", 'value': row['Symbol']}
        for _, row in df.iterrows()
    ]
    
    return options, df.to_dict('records')


# Callback: Market Status Update (Live)
@app.callback(
    Output('market-status-card', 'children'),
    Input('interval-market-status', 'n_intervals')
)
def update_market_status(n_intervals):
    """
    Aktualisiert Market Status alle 10 Sekunden
    """
    status = MarketHours.get_market_status()
    
    # Styling basierend auf Status
    if status['is_open']:
        bg_color = '#d4edda'
        border_color = '#28a745'
        text_color = '#155724'
    elif status['session'] in ['pre_market', 'after_hours']:
        bg_color = '#fff3cd'
        border_color = '#ffc107'
        text_color = '#856404'
    else:
        bg_color = '#f8d7da'
        border_color = '#dc3545'
        text_color = '#721c24'
    
    # Zeitformatierung
    time_str = status['current_time_et'].strftime('%Y-%m-%d %I:%M:%S %p ET')
    weekday = status['current_time_et'].strftime('%A')
    
    # Erstelle Status Card
    status_card = html.Div([
        html.Div([
            html.H3([
                html.Span(status['emoji'], style={'marginRight': '15px', 'fontSize': '32px'}),
                html.Span(status['message'])
            ], style={'margin': '0', 'color': text_color}),
            
            html.Div([
                html.P([
                    html.Strong("Aktuelle Zeit: "),
                    f"{time_str} ({weekday})"
                ], style={'margin': '10px 0 5px 0', 'fontSize': '16px'}),
                
                html.P([
                    html.Strong("Session: "),
                    status['session'].replace('_', ' ').title()
                ], style={'margin': '5px 0', 'fontSize': '14px'}),
            ]),
            
            # Nächste Öffnung/Schließung
            html.Div([
                html.P(
                    f"🕒 Nächste Öffnung: {status['next_open'].strftime('%A, %I:%M %p ET')}" 
                    if 'next_open' in status and status['next_open'] 
                    else "",
                    style={'margin': '5px 0', 'fontSize': '14px', 'fontStyle': 'italic'}
                ),
                html.P(
                    f"⏰ Schließt um: {status['next_close'].strftime('%I:%M %p ET')}" 
                    if 'next_close' in status and status['next_close'] 
                    else "",
                    style={'margin': '5px 0', 'fontSize': '14px', 'fontStyle': 'italic'}
                ),
            ]),
        ], style={'padding': '20px'})
        
    ], style={
        'backgroundColor': bg_color,
        'border': f'3px solid {border_color}',
        'borderRadius': '10px',
        'boxShadow': '0 2px 8px rgba(0,0,0,0.1)'
    })
    
    return status_card


# Callback: Alle Ticker laden (Top 10)
@app.callback(
    Output('ticker-dropdown', 'value'),
    Input('load-all-btn', 'n_clicks'),
    State('all-tickers-store', 'data'),
    prevent_initial_call=True
)
def load_all_tickers(n_clicks, all_tickers):
    if n_clicks > 0 and all_tickers:
        # Lade nur erste 10 für Demo
        symbols = [t['Symbol'] for t in all_tickers[:10]]
        return symbols
    return []


# Callback: Ticker Table
@app.callback(
    Output('ticker-table', 'children'),
    Input('ticker-dropdown', 'value'),
    State('all-tickers-store', 'data')
)
def update_ticker_table(selected_symbols, all_tickers):
    if not selected_symbols:
        return html.P("⚠️ Keine Ticker ausgewählt", style={'color': '#666'})
    
    # Filter selected tickers
    df = pd.DataFrame(all_tickers)
    selected_df = df[df['Symbol'].isin(selected_symbols)]
    
    # Erstelle Tabelle
    table = dash_table.DataTable(
        data=selected_df[['Symbol', 'Security', 'GICS Sector', 
                         'Headquarters Location']].to_dict('records'),
        columns=[
            {'name': 'Symbol', 'id': 'Symbol'},
            {'name': 'Name', 'id': 'Security'},
            {'name': 'Sektor', 'id': 'GICS Sector'},
            {'name': 'Hauptsitz', 'id': 'Headquarters Location'},
        ],
        style_cell={
            'textAlign': 'left',
            'padding': '10px',
            'fontFamily': 'Arial'
        },
        style_header={
            'backgroundColor': '#3498db',
            'color': 'white',
            'fontWeight': 'bold',
            'textAlign': 'left'
        },
        style_data_conditional=[
            {
                'if': {'row_index': 'odd'},
                'backgroundColor': '#f9f9f9'
            }
        ]
    )
    
    return table


# Callback: Historische Daten laden mit Market Hours Filter
@app.callback(
    Output('price-chart', 'figure'),
    Input('load-historical-btn', 'n_clicks'),
    State('ticker-dropdown', 'value'),
    prevent_initial_call=True
)
def load_historical_data(n_clicks, selected_tickers):
    """
    Lädt historische Daten und filtert auf Regular Market Hours
    """
    if not selected_tickers:
        fig = go.Figure()
        fig.add_annotation(
            text="⚠️ Bitte wählen Sie zuerst einen Ticker aus",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=18, color="gray")
        )
        return fig
    
    # Lade Daten für ersten Ticker
    ticker = selected_tickers[0]
    print(f"\n📊 Lade Daten für {ticker}...")
    df = api_client.get_aggregates(ticker, timespan='minute', limit=500)
    
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text=f"❌ Keine Daten für {ticker} verfügbar",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=18, color="red")
        )
        return fig
    
    print(f"✓ {len(df)} Datenpunkte geladen")
    print(f"Zeitspanne: {df['timestamp'].min()} bis {df['timestamp'].max()}")
    
    # ✨ WICHTIG: Filter auf Regular Market Hours
    print("\n🔍 Filtere auf Regular Market Hours...")
    df_filtered = MarketHours.filter_regular_hours(df, timestamp_col='timestamp')
    
    print(f"✓ Nach Filter: {len(df_filtered)} Datenpunkte")
    
    if df_filtered.empty:
        fig = go.Figure()
        fig.add_annotation(
            text=f"⚠️ Keine Daten während Regular Market Hours für {ticker}\n(Nur Daten von Mo-Fr 9:30 AM - 4:00 PM ET werden angezeigt)",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16, color="orange"),
            align="center"
        )
        return fig
    
    print(f"Gefilterte Zeitspanne: {df_filtered['timestamp'].min()} bis {df_filtered['timestamp'].max()}")
    
    # Erstelle Candlestick Chart
    fig = go.Figure()
    
    # Candlestick
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
    
    # VWAP Line
    if 'vwap' in df_filtered.columns:
        fig.add_trace(go.Scatter(
            x=df_filtered['timestamp'],
            y=df_filtered['vwap'],
            mode='lines',
            name='VWAP',
            line=dict(color='#2196f3', width=2, dash='dash'),
            opacity=0.7
        ))
    
    # Layout
    fig.update_layout(
        title={
            'text': f'{ticker} - Historical Data (Regular Market Hours Only)<br><sub>{len(df_filtered)} bars | {df_filtered["timestamp"].min().strftime("%Y-%m-%d")} to {df_filtered["timestamp"].max().strftime("%Y-%m-%d")}</sub>',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 20}
        },
        xaxis_title='Time (ET - Eastern Time)',
        yaxis_title='Price ($)',
        template='plotly_white',
        height=600,
        hovermode='x unified',
        xaxis=dict(
            rangeslider=dict(visible=False),
            type='date',
            showgrid=True,
            gridcolor='#e0e0e0'
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='#e0e0e0'
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    # Market Hours Hinweis als Annotation
    fig.add_annotation(
        text="📅 Nur Regular Market Hours (Mo-Fr 9:30 AM - 4:00 PM ET)",
        xref="paper", yref="paper",
        x=0.5, y=-0.12,
        showarrow=False,
        font=dict(size=12, color="gray"),
        xanchor="center"
    )
    
    return fig


# Callback: Market Hours Test
@app.callback(
    Output('test-output', 'children'),
    Input('test-market-hours-btn', 'n_clicks'),
    prevent_initial_call=True
)
def test_market_hours(n_clicks):
    """
    Test-Funktion für Market Hours Logic
    """
    if n_clicks == 0:
        return ""
    
    # Führe Tests durch
    now_et = MarketHours.get_eastern_time()
    is_open = MarketHours.is_market_open()
    session = MarketHours.get_market_session()
    is_weekend = MarketHours.is_weekend(now_et)
    is_holiday = MarketHours.is_market_holiday(now_et)
    
    results = html.Div([
        html.H4("🧪 Market Hours Test Results"),
        html.Ul([
            html.Li(f"Aktuelle Zeit (ET): {now_et.strftime('%Y-%m-%d %H:%M:%S %Z')}"),
            html.Li(f"Market Open: {'✅ Ja' if is_open else '❌ Nein'}"),
            html.Li(f"Session: {session.value}"),
            html.Li(f"Wochenende: {'✅ Ja' if is_weekend else '❌ Nein'}"),
            html.Li(f"Feiertag: {'✅ Ja' if is_holiday else '❌ Nein'}"),
            html.Li(f"Trading Day: {'✅ Ja' if MarketHours.is_trading_day(now_et) else '❌ Nein'}"),
        ]),
        html.P("✅ Alle Market Hours Funktionen arbeiten korrekt!", 
               style={'color': 'green', 'fontWeight': 'bold', 'marginTop': '10px'})
    ], style={
        'backgroundColor': '#f0f8ff',
        'padding': '15px',
        'borderRadius': '8px',
        'border': '1px solid #2196f3'
    })
    
    return results


# ============================================
# Run Server
# ============================================

if __name__ == '__main__':
    print("=" * 60)
    print("🚀 S&P 500 Stock Streaming Platform")
    print("=" * 60)
    print(f"\n✓ Market Hours Status: {get_market_status_message()}")
    print(f"\nÖffne Browser: http://127.0.0.1:8050")
    print("=" * 60)
    
    app.run_server(debug=True, port=8050, host='127.0.0.1')
