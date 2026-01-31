# src/visualization/app.py
"""
S&P 500 Stock Streaming Platform - PHASE 6 COMPLETE
- Volume Chart unterhalb Main Chart
- Historische 600 Punkte Pre-Loading
- Rolling Window für Sekunden
- VWAP Toggle
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
    html.H1("📊 S&P 500 Stock Streaming Platform - Phase 6", 
            style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': '20px'}),
    
    dcc.Tabs(id='main-tabs', value='streaming-tab', children=[
        dcc.Tab(label='🔴 Streaming (Live)', value='streaming-tab'),
        dcc.Tab(label='📈 Historische Daten', value='historical-tab', disabled=True),
        dcc.Tab(label='🤖 Machine Learning', value='ml-tab', disabled=True),
    ]),
    
    html.Div(id='tab-content', style={'padding': '20px'}),
    
    # Intervals
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
        html.H2("🎯 Real-time Stock Streaming (Phase 6 Complete)"),
        
        # Market Status
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
                           style={'marginTop': '10px'}),
            ], style={'width': '48%', 'display': 'inline-block', 'verticalAlign': 'top'}),
            
            # Right: Streaming Controls
            html.Div([
                html.H3("🎮 Streaming Controls"),
                
                # Aggregation
                html.Label("Aggregation:", style={'fontWeight': 'bold'}),
                dcc.RadioItems(
                    id='aggregation-selector',
                    options=[
                        {'label': ' Minuten (AM.*)', 'value': 'minute'},
                        {'label': ' Sekunden (A.*)', 'value': 'second'}
                    ],
                    value='minute',
                    inline=True,
                    style={'marginBottom': '15px'}
                ),
                
                # Chart Type
                html.Label("Chart Type:", style={'fontWeight': 'bold'}),
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
                    dcc.Checklist(
                        id='vwap-toggle',
                        options=[{'label': ' VWAP anzeigen', 'value': 'show'}],
                        value=['show'],
                        inline=True
                    )
                ], style={'marginBottom': '15px'}),
                
                # Chart Ticker
                html.Div([
                    html.Label("📊 Chart Ticker:", style={'fontWeight': 'bold'}),
                    dcc.Dropdown(
                        id='chart-ticker-dropdown',
                        placeholder='Ticker wählen...',
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
        
        # Statistics
        html.Div(id='streaming-stats', style={
            'padding': '15px', 'backgroundColor': '#f8f9fa', 'borderRadius': '5px',
            'marginBottom': '20px'
        }),
        
        # Ticker Table
        html.Div([
            html.H3("✅ Ausgewählte Ticker"),
            html.Div(id='ticker-table')
        ], style={'marginTop': '30px', 'marginBottom': '30px'}),
        
        # Charts mit Subplots (Main + Volume)
        html.Div([
            html.H3("📈 Live Chart mit Volume"),
            dcc.Graph(id='combined-chart', style={'height': '800px'})
        ]),
        
        # Hidden Stores
        dcc.Store(id='all-tickers-store'),
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
    return html.Div("Tab in Entwicklung...")


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


@app.callback(
    Output('ticker-dropdown', 'value'),
    Input('load-top10-btn', 'n_clicks'),
    prevent_initial_call=True
)
def load_top_tickers(n_clicks):
    if n_clicks > 0:
        return ['AAPL', 'MSFT', 'GOOGL']  # Nur 3 für Demo
    return []


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
def update_ticker_table(selected_symbols, n_intervals, all_tickers, streaming_state):
    if not selected_symbols:
        return html.P("⚠️ Keine Ticker ausgewählt", style={'color': '#666'})
    
    df = pd.DataFrame(all_tickers)
    selected_df = df[df['Symbol'].isin(selected_symbols)].copy()
    
    # Live-Daten hinzufügen
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
        style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': '#f9f9f9'}]
    )
    return table


# ✨ START STREAMING mit Pre-Load
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
    
    # START
    if triggered_id == 'start-streaming-btn':
        if not selected_tickers:
            return state, False, True, True, True, html.Span("⚠️ Keine Ticker", style={'color': 'orange'})
        
        try:
            print(f"\n🚀 Starte Streaming mit Pre-Load...")
            print(f"   Ticker: {selected_tickers}")
            print(f"   Aggregation: {aggregation}")
            
            # 1. Initialize Stream Manager
            max_points = 600 if aggregation == 'second' else 600
            stream_manager = StreamDataManager(max_points=max_points)
            current_aggregation = aggregation
            
            # 2. ✨ Pre-Load historische Daten (nur bei Minuten)
            if aggregation == 'minute':
                print("\n📊 Lade historische 600 Minuten-Punkte...")
                for ticker in selected_tickers:
                    try:
                        # Lade letzte 600 Minuten (~2.5 Handelstage)
                        from_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
                        to_date = datetime.now().strftime('%Y-%m-%d')
                        
                        df_hist = api_client.get_aggregates(
                            ticker, 
                            timespan='minute',
                            from_date=from_date,
                            to_date=to_date,
                            limit=600
                        )
                        
                        if not df_hist.empty:
                            # Filter auf Market Hours
                            df_hist = MarketHours.filter_regular_hours(df_hist)
                            # Nehme nur die letzten 600
                            df_hist = df_hist.tail(600)
                            
                            # Pre-load in Buffer
                            stream_manager.preload_historical_data(ticker, df_hist)
                        
                    except Exception as e:
                        print(f"⚠️ Pre-Load für {ticker} fehlgeschlagen: {e}")
            
            # 3. Initialize WebSocket
            ws_client = StockWebSocketClient(aggregation=aggregation)
            
            # Set Callback
            def handle_message(data):
                symbol = data.get('symbol')
                if symbol in selected_tickers:
                    stream_manager.add_data_point(symbol, data)
            
            ws_client.set_message_callback(handle_message)
            
            # 4. Subscribe & Start
            ws_client.subscribe(selected_tickers)
            ws_client.start()
            
            streaming_active = True
            
            new_state = {'active': True, 'aggregation': aggregation}
            status = html.Span("🟢 Streaming AKTIV", 
                             style={'color': 'green', 'fontWeight': 'bold'})
            
            return new_state, True, False, False, False, status
            
        except Exception as e:
            print(f"✗ Streaming Fehler: {e}")
            import traceback
            traceback.print_exc()
            
            status = html.Span(f"❌ Fehler: {str(e)}", style={'color': 'red'})
            return state, False, True, True, True, status
    
    # STOP
    elif triggered_id == 'stop-streaming-btn':
        if ws_client:
            ws_client.stop()
        
        streaming_active = False
        ws_client = None
        stream_manager = None
        
        new_state = {'active': False, 'aggregation': 'minute'}
        status = html.Span("⚫ Gestoppt", style={'color': 'gray'})
        
        return new_state, False, True, True, True, status
    
    return state, False, True, True, True, ""


# Statistics
@app.callback(
    Output('streaming-stats', 'children'),
    Input('interval-stats', 'n_intervals'),
    State('streaming-state', 'data'),
    State('chart-ticker-dropdown', 'value')
)
def update_streaming_stats(n_intervals, state, chart_ticker):
    if not state['active'] or not stream_manager:
        return html.P("Streaming nicht aktiv", style={'color': '#666'})
    
    stats = stream_manager.get_statistics()
    
    # Buffer Status für Chart-Ticker
    buffer_info = ""
    if chart_ticker:
        buffer_status = stream_manager.get_buffer_status(chart_ticker)
        buffer_info = html.Div([
            html.Strong(f"Buffer Status ({chart_ticker}): "),
            html.Span(f"{buffer_status['size']}/{buffer_status['max']} Punkte "),
            html.Span(f"({buffer_status['percentage']}%) ", 
                     style={'color': 'green' if buffer_status['is_full'] else 'orange'}),
            html.Span(f"| Pre-loaded: {buffer_status['preloaded_count']}, Live: {buffer_status['live_count']}")
        ], style={'marginTop': '10px', 'fontSize': '14px'})
    
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
        buffer_info
    ])


# ✨ COMBINED CHART (Main + Volume)
@app.callback(
    Output('combined-chart', 'figure'),
    Input('interval-live-chart', 'n_intervals'),
    State('chart-ticker-dropdown', 'value'),
    State('chart-type-selector', 'value'),
    State('vwap-toggle', 'value'),
    State('streaming-state', 'data')
)
def update_combined_chart(n_intervals, ticker, chart_type, vwap_values, state):
    if not state['active'] or not stream_manager or not ticker:
        # Empty chart
        fig = make_subplots(
            rows=2, cols=1,
            row_heights=[0.7, 0.3],
            vertical_spacing=0.03,
            subplot_titles=('Warte auf Streaming-Daten...', '')
        )
        return fig
    
    # Get Data
    df = stream_manager.get_dataframe(ticker)
    
    if df.empty:
        fig = make_subplots(rows=2, cols=1, row_heights=[0.7, 0.3])
        fig.add_annotation(text=f"⏳ Warte auf Daten für {ticker}...",
                          xref="paper", yref="paper", x=0.5, y=0.5,
                          showarrow=False, font=dict(size=16, color="orange"))
        return fig
    
    # Filter Market Hours
    df_filtered = MarketHours.filter_regular_hours(df, timestamp_col='timestamp')
    if df_filtered.empty:
        df_filtered = df
    
    # Buffer Status
    buffer_status = stream_manager.get_buffer_status(ticker)
    
    # Create Subplots
    fig = make_subplots(
        rows=2, cols=1,
        row_heights=[0.75, 0.25],
        vertical_spacing=0.03,
        subplot_titles=(
            f'{ticker} - Live {state["aggregation"].title()} | Buffer: {buffer_status["size"]}/{buffer_status["max"]}',
            'Volume'
        ),
        specs=[[{"secondary_y": False}], [{"secondary_y": False}]]
    )
    
    # Main Chart
    if chart_type == 'candlestick':
        fig.add_trace(
            go.Candlestick(
                x=df_filtered['timestamp'],
                open=df_filtered['open'],
                high=df_filtered['high'],
                low=df_filtered['low'],
                close=df_filtered['close'],
                name='OHLC',
                increasing_line_color='#26a69a',
                decreasing_line_color='#ef5350'
            ),
            row=1, col=1
        )
    else:  # line
        fig.add_trace(
            go.Scatter(
                x=df_filtered['timestamp'],
                y=df_filtered['close'],
                mode='lines',
                name='Close',
                line=dict(color='#2196f3', width=2)
            ),
            row=1, col=1
        )
    
    # VWAP
    if 'show' in vwap_values and 'vwap' in df_filtered.columns:
        fig.add_trace(
            go.Scatter(
                x=df_filtered['timestamp'],
                y=df_filtered['vwap'],
                mode='lines',
                name='VWAP',
                line=dict(color='#ff9800', width=2, dash='dash'),
                opacity=0.8
            ),
            row=1, col=1
        )
    
    # Volume Chart
    colors = ['#26a69a' if close >= open else '#ef5350' 
              for close, open in zip(df_filtered['close'], df_filtered['open'])]
    
    fig.add_trace(
        go.Bar(
            x=df_filtered['timestamp'],
            y=df_filtered['volume'],
            name='Volume',
            marker=dict(color=colors, opacity=0.6),
            showlegend=False
        ),
        row=2, col=1
    )
    
    # Layout
    fig.update_layout(
        height=800,
        template='plotly_white',
        hovermode='x unified',
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=50, b=50)
    )
    
    # X-Axis (nur für Volume zeigen)
    fig.update_xaxes(showgrid=True, gridcolor='#e0e0e0', row=1, col=1)
    fig.update_xaxes(title_text="Zeit (ET)", showgrid=True, gridcolor='#e0e0e0', row=2, col=1)
    
    # Y-Axis
    fig.update_yaxes(title_text="Preis ($)", showgrid=True, gridcolor='#e0e0e0', row=1, col=1)
    fig.update_yaxes(title_text="Volume", showgrid=False, row=2, col=1)
    
    return fig


# ============================================
# Run Server
# ============================================

if __name__ == '__main__':
    print("=" * 60)
    print("🚀 S&P 500 Stock Streaming Platform - PHASE 6")
    print("=" * 60)
    print(f"\n✓ Market Status: {get_market_status_message()}")
    print(f"\n🌐 Browser: http://127.0.0.1:8050")
    print("=" * 60)
    print("\n✨ Phase 6 Features:")
    print("  • Combined Chart (Price + Volume)")
    print("  • Pre-Loading von 600 historischen Minuten")
    print("  • Rolling Window für Sekunden (600 Punkte)")
    print("  • VWAP Toggle")
    print("  • Buffer Status Anzeige")
    print("  • Live-Updates alle 2 Sekunden")
    print("\n💡 Tipp: Am Montag während Market Hours testen!")
    print("=" * 60)
    
    app.run_server(debug=True, port=8050, host='127.0.0.1')
