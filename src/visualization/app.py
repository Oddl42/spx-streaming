# src/visualization/app.py
"""
S&P 500 Stock Streaming Platform - Full Application
MIT LIVE WEBSOCKET STREAMING & REAL-TIME CHARTS
"""

import dash
from dash import dcc, html, Input, Output, State, dash_table, ctx, ALL
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import pandas as pd
from pathlib import Path
import sys
import time
from datetime import datetime

# Füge src zum Path hinzu
sys.path.append(str(Path(__file__).parent.parent.parent))

from src.utils.helpers import SP500TickerLoader
from src.api.massive_rest_client import MassiveRESTClient
from src.utils.market_hours import MarketHours, is_market_open, get_market_status_message
from src.streaming.websocket_client import StockWebSocketClient
from src.streaming.stream_manager import StreamDataManager

# ============================================
# App & Globals
# ============================================

app = dash.Dash(__name__, suppress_callback_exceptions=True)
app.title = "S&P 500 Stock Streaming"

# Globals
ticker_loader = SP500TickerLoader()
api_client = MassiveRESTClient()

# Streaming Components (Global)
ws_client: StockWebSocketClient = None
stream_manager: StreamDataManager = None
streaming_active = False
current_aggregation = 'minute'

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
    
    # Intervals für Updates
    dcc.Interval(id='interval-market-status', interval=10*1000, n_intervals=0),
    dcc.Interval(id='interval-live-chart', interval=2*1000, n_intervals=0, disabled=True),
    dcc.Interval(id='interval-stats', interval=5*1000, n_intervals=0, disabled=True),
    
    # Stores
    dcc.Store(id='streaming-state', data={'active': False, 'aggregation': 'minute'}),
    
], style={'fontFamily': 'Arial, sans-serif'})


# ============================================
# Streaming Tab Layout
# ============================================

def create_streaming_tab():
    return html.Div([
        html.H2("🎯 Real-time Stock Streaming with WebSocket"),
        
        # Market Hours Status
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
                html.Button('Top 10 laden', id='load-top10-btn', n_clicks=0,
                           style={'marginTop': '10px', 'marginRight': '10px'}),
            ], style={'width': '48%', 'display': 'inline-block', 'verticalAlign': 'top'}),
            
            # Right: Streaming Controls
            html.Div([
                html.H3("🎮 Streaming Controls"),
                
                # Aggregation Selection
                html.Label("Aggregation:", style={'fontWeight': 'bold', 'marginRight': '10px'}),
                dcc.RadioItems(
                    id='aggregation-selector',
                    options=[
                        {'label': ' Minuten-Aggregation (AM.*)', 'value': 'minute'},
                        {'label': ' Sekunden-Aggregation (A.*)', 'value': 'second'}
                    ],
                    value='minute',
                    inline=True,
                    style={'marginBottom': '15px'}
                ),
                
                # Chart Type
                html.Label("Chart Type:", style={'fontWeight': 'bold', 'marginRight': '10px'}),
                dcc.RadioItems(
                    id='chart-type-selector',
                    options=[
                        {'label': ' Candlestick', 'value': 'candlestick'},
                        {'label': ' Line Chart', 'value': 'line'}
                    ],
                    value='candlestick',
                    inline=True,
                    style={'marginBottom': '15px'}
                ),
                
                # VWAP Toggle
                html.Div([
                    html.Label("VWAP:", style={'fontWeight': 'bold', 'marginRight': '10px'}),
                    dcc.Checklist(
                        id='vwap-toggle',
                        options=[{'label': ' Anzeigen', 'value': 'show'}],
                        value=[],
                        inline=True
                    )
                ], style={'marginBottom': '15px'}),
                
                # Chart Ticker Selection
                html.Div([
                    html.Label("📊 Ticker für Chart:", style={'fontWeight': 'bold'}),
                    dcc.Dropdown(
                        id='chart-ticker-dropdown',
                        placeholder='Wählen Sie einen Ticker...',
                        clearable=False
                    )
                ], id='chart-ticker-selector', style={'marginTop': '10px', 'display': 'none'}),
                
            ], style={'width': '48%', 'display': 'inline-block', 'verticalAlign': 'top', 
                     'marginLeft': '4%'}),
        ]),
        
        # Streaming Buttons
        html.Div([
            html.Button('🟢 Streaming Starten', id='start-streaming-btn', n_clicks=0,
                       style={'padding': '12px 30px', 'fontSize': '16px', 'marginRight': '10px',
                             'backgroundColor': '#28a745', 'color': 'white', 'border': 'none',
                             'borderRadius': '5px', 'cursor': 'pointer'}),
            html.Button('🔴 Streaming Stoppen', id='stop-streaming-btn', n_clicks=0, disabled=True,
                       style={'padding': '12px 30px', 'fontSize': '16px',
                             'backgroundColor': '#dc3545', 'color': 'white', 'border': 'none',
                             'borderRadius': '5px'}),
            html.Span(id='streaming-status', style={'marginLeft': '20px', 'fontSize': '16px'})
        ], style={'marginTop': '20px', 'marginBottom': '20px'}),
        
        # Statistics Box
        html.Div(id='streaming-stats', style={
            'padding': '15px', 'backgroundColor': '#f8f9fa', 'borderRadius': '5px',
            'marginBottom': '20px'
        }),
        
        # Ticker Table
        html.Div([
            html.H3("✅ Ausgewählte Ticker"),
            html.Div(id='ticker-table')
        ], style={'marginTop': '30px', 'marginBottom': '30px'}),
        
        # Live Charts
        html.Div([
            html.H3("📈 Live Chart"),
            dcc.Graph(id='live-chart', style={'height': '600px'}),
            dcc.Graph(id='volume-chart', style={'height': '200px'})
        ]),
        
        # Hidden Stores
        dcc.Store(id='all-tickers-store'),
        dcc.Store(id='selected-tickers-store'),
    ])


# ============================================
# Callbacks
# ============================================

# Tab Content
@app.callback(
    Output('tab-content', 'children'),
    Input('main-tabs', 'value')
)
def render_tab_content(active_tab):
    if active_tab == 'streaming-tab':
        return create_streaming_tab()
    return html.Div("Tab in Entwicklung...")


# Load Ticker Options
@app.callback(
    Output('ticker-dropdown', 'options'),
    Output('all-tickers-store', 'data'),
    Input('main-tabs', 'value')
)
def load_ticker_options(tab):
    df = ticker_loader.load_tickers()
    options = [
        {'label': f"{row['Symbol']} - {row['Security']}", 'value': row['Symbol']}
        for _, row in df.iterrows()
    ]
    return options, df.to_dict('records')


# Market Status Update
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
    weekday = status['current_time_et'].strftime('%A')
    
    return html.Div([
        html.Div([
            html.H3([
                html.Span(status['emoji'], style={'marginRight': '15px', 'fontSize': '32px'}),
                html.Span(status['message'])
            ], style={'margin': '0', 'color': text_color}),
            html.P(f"Zeit: {time_str} ({weekday})", 
                  style={'margin': '10px 0', 'fontSize': '16px'}),
        ], style={'padding': '20px'})
    ], style={'backgroundColor': bg_color, 'border': f'3px solid {border_color}',
             'borderRadius': '10px', 'boxShadow': '0 2px 8px rgba(0,0,0,0.1)'})


# Load Top 10 Tickers
@app.callback(
    Output('ticker-dropdown', 'value'),
    Input('load-top10-btn', 'n_clicks'),
    State('all-tickers-store', 'data'),
    prevent_initial_call=True
)
def load_top_tickers(n_clicks, all_tickers):
    if n_clicks > 0 and all_tickers:
        # Top 10: AAPL, MSFT, GOOGL, AMZN, TSLA, META, NVDA, JPM, V, JNJ
        top_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'V', 'JNJ']
        return top_symbols[:5]  # Nur 5 für Demo (weniger API Last)
    return []


# Update Chart Ticker Dropdown
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


# Ticker Table
@app.callback(
    Output('ticker-table', 'children'),
    Input('ticker-dropdown', 'value'),
    Input('interval-live-chart', 'n_intervals'),
    State('all-tickers-store', 'data'),
    State('streaming-state', 'data')
)
def update_ticker_table(selected_symbols, n_intervals, all_tickers, streaming_state):
    if not selected_symbols:
        return html.P("⚠️ Keine Ticker ausgewählt", style={'color': '#666'})
    
    df = pd.DataFrame(all_tickers)
    selected_df = df[df['Symbol'].isin(selected_symbols)].copy()
    
    # Wenn Streaming aktiv, füge Live-Daten hinzu
    if streaming_state['active'] and stream_manager:
        for idx, row in selected_df.iterrows():
            symbol = row['Symbol']
            latest = stream_manager.get_latest_point(symbol)
            if latest:
                selected_df.at[idx, 'Last Price'] = f"${latest.get('close', 0):.2f}"
                selected_df.at[idx, 'Volume'] = f"{latest.get('volume', 0):,}"
                selected_df.at[idx, 'VWAP'] = f"${latest.get('vwap', 0):.2f}"
            else:
                selected_df.at[idx, 'Last Price'] = "N/A"
                selected_df.at[idx, 'Volume'] = "N/A"
                selected_df.at[idx, 'VWAP'] = "N/A"
        
        columns = ['Symbol', 'Security', 'GICS Sector', 'Last Price', 'Volume', 'VWAP']
    else:
        columns = ['Symbol', 'Security', 'GICS Sector', 'Headquarters Location']
    
    table = dash_table.DataTable(
        data=selected_df[columns].to_dict('records'),
        columns=[{'name': col, 'id': col} for col in columns],
        style_cell={'textAlign': 'left', 'padding': '10px'},
        style_header={'backgroundColor': '#3498db', 'color': 'white', 'fontWeight': 'bold'},
        style_data_conditional=[
            {'if': {'row_index': 'odd'}, 'backgroundColor': '#f9f9f9'}
        ]
    )
    return table


# Start Streaming
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
    
    # Start Streaming
    if triggered_id == 'start-streaming-btn':
        if not selected_tickers:
            return state, False, True, True, True, html.Span("⚠️ Keine Ticker ausgewählt", 
                                                             style={'color': 'orange'})
        
        try:
            print(f"\n🚀 Starte WebSocket Streaming...")
            print(f"   Ticker: {selected_tickers}")
            print(f"   Aggregation: {aggregation}")
            
            # Initialize Components
            stream_manager = StreamDataManager(max_points=600)
            ws_client = StockWebSocketClient(aggregation=aggregation)
            current_aggregation = aggregation
            
            # Set Message Callback
            def handle_message(data):
                symbol = data.get('symbol')
                if symbol in selected_tickers:
                    stream_manager.add_data_point(symbol, data)
            
            ws_client.set_message_callback(handle_message)
            
            # Subscribe & Start
            ws_client.subscribe(selected_tickers)
            ws_client.start()
            
            streaming_active = True
            
            new_state = {'active': True, 'aggregation': aggregation}
            status = html.Span("🟢 Streaming AKTIV", 
                             style={'color': 'green', 'fontWeight': 'bold'})
            
            return new_state, True, False, False, False, status
            
        except Exception as e:
            print(f"✗ Streaming Start Fehler: {e}")
            import traceback
            traceback.print_exc()
            
            status = html.Span(f"❌ Fehler: {str(e)}", style={'color': 'red'})
            return state, False, True, True, True, status
    
    # Stop Streaming
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


# Streaming Statistics
@app.callback(
    Output('streaming-stats', 'children'),
    Input('interval-stats', 'n_intervals'),
    State('streaming-state', 'data')
)
def update_streaming_stats(n_intervals, state):
    if not state['active'] or not stream_manager:
        return html.P("Streaming nicht aktiv", style={'color': '#666'})
    
    stats = stream_manager.get_statistics()
    
    return html.Div([
        html.H4("📊 Streaming Statistiken", style={'marginBottom': '10px'}),
        html.Div([
            html.Div([
                html.Strong("Gesamte Nachrichten: "),
                html.Span(f"{stats['total_messages']:,}")
            ], style={'display': 'inline-block', 'marginRight': '30px'}),
            
            html.Div([
                html.Strong("Aktive Ticker: "),
                html.Span(f"{stats['active_tickers']}")
            ], style={'display': 'inline-block', 'marginRight': '30px'}),
            
            html.Div([
                html.Strong("Aggregation: "),
                html.Span(current_aggregation.title())
            ], style={'display': 'inline-block'}),
        ]),
        
        html.Div([
            html.P("Buffer Status:", style={'fontWeight': 'bold', 'marginTop': '10px', 
                                           'marginBottom': '5px'}),
            html.Ul([
                html.Li(f"{symbol}: {size} Datenpunkte")
                for symbol, size in stats['buffer_sizes'].items()
            ])
        ]) if stats['buffer_sizes'] else None
    ])


# Live Chart Update
@app.callback(
    Output('live-chart', 'figure'),
    Output('volume-chart', 'figure'),
    Input('interval-live-chart', 'n_intervals'),
    State('chart-ticker-dropdown', 'value'),
    State('chart-type-selector', 'value'),
    State('vwap-toggle', 'value'),
    State('streaming-state', 'data')
)
def update_live_chart(n_intervals, ticker, chart_type, vwap_values, state):
    if not state['active'] or not stream_manager or not ticker:
        # Empty charts
        fig_main = go.Figure()
        fig_main.add_annotation(text="⏸️ Warte auf Streaming-Daten...",
                               xref="paper", yref="paper", x=0.5, y=0.5,
                               showarrow=False, font=dict(size=16, color="gray"))
        
        fig_volume = go.Figure()
        return fig_main, fig_volume
    
    # Get Data
    df = stream_manager.get_dataframe(ticker)
    
    if df.empty:
        fig_main = go.Figure()
        fig_main.add_annotation(text=f"⏳ Warte auf Daten für {ticker}...",
                               xref="paper", yref="paper", x=0.5, y=0.5,
                               showarrow=False, font=dict(size=16, color="orange"))
        fig_volume = go.Figure()
        return fig_main, fig_volume
    
    # Filter Market Hours
    df_filtered = MarketHours.filter_regular_hours(df, timestamp_col='timestamp')
    
    if df_filtered.empty:
        df_filtered = df  # Use unfiltered if no market hours data
    
    # Main Chart
    fig_main = go.Figure()
    
    if chart_type == 'candlestick':
        fig_main.add_trace(go.Candlestick(
            x=df_filtered['timestamp'],
            open=df_filtered['open'],
            high=df_filtered['high'],
            low=df_filtered['low'],
            close=df_filtered['close'],
            name='OHLC',
            increasing_line_color='#26a69a',
            decreasing_line_color='#ef5350'
        ))
    else:  # line
        fig_main.add_trace(go.Scatter(
            x=df_filtered['timestamp'],
            y=df_filtered['close'],
            mode='lines',
            name='Close',
            line=dict(color='#2196f3', width=2)
        ))
    
    # VWAP
    show_vwap = 'show' in vwap_values
    if show_vwap and 'vwap' in df_filtered.columns:
        fig_main.add_trace(go.Scatter(
            x=df_filtered['timestamp'],
            y=df_filtered['vwap'],
            mode='lines',
            name='VWAP',
            line=dict(color='#ff9800', width=2, dash='dash'),
            opacity=0.8
        ))
    
    # Layout
    buffer_status = f" | Buffer: {len(df_filtered)}/600" if state['aggregation'] == 'second' else ""
    
    fig_main.update_layout(
        title=f'{ticker} - Live {state["aggregation"].title()} Data{buffer_status}',
        xaxis_title='Zeit (ET)',
        yaxis_title='Preis ($)',
        template='plotly_white',
        height=600,
        hovermode='x unified',
        xaxis=dict(rangeslider=dict(visible=False), showgrid=True),
        yaxis=dict(showgrid=True),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    # Volume Chart
    fig_volume = go.Figure()
    
    colors = ['#26a69a' if close >= open else '#ef5350' 
              for close, open in zip(df_filtered['close'], df_filtered['open'])]
    
    fig_volume.add_trace(go.Bar(
        x=df_filtered['timestamp'],
        y=df_filtered['volume'],
        name='Volume',
        marker=dict(color=colors, opacity=0.7)
    ))
    
    fig_volume.update_layout(
        title='Volume',
        xaxis_title='',
        yaxis_title='Volume',
        template='plotly_white',
        height=200,
        margin=dict(t=40, b=0),
        showlegend=False
    )
    
    return fig_main, fig_volume


# ============================================
# Run Server
# ============================================

if __name__ == '__main__':
    print("=" * 60)
    print("🚀 S&P 500 Stock Streaming Platform")
    print("=" * 60)
    print(f"\n✓ Market Status: {get_market_status_message()}")
    print(f"\n🌐 Öffne Browser: http://127.0.0.1:8050")
    print("=" * 60)
    print("\n📝 Features:")
    print("  • Live WebSocket Streaming (Minuten & Sekunden)")
    print("  • Real-time Candlestick & Line Charts")
    print("  • Volume Chart")
    print("  • VWAP Indikator")
    print("  • Market Hours Filtering")
    print("  • Rolling Window (600 Punkte bei Sekunden)")
    print("\n⚠️  Hinweis: Massive.com API Key in .env erforderlich!")
    print("=" * 60)
    
    app.run_server(debug=True, port=8050, host='127.0.0.1')
