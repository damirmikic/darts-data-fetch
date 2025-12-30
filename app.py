#!/usr/bin/env python3
"""
Sofascore Darts Data Viewer - Streamlit App
Interactive web application for fetching and viewing darts match statistics
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import time
import json
import random
import plotly.graph_objects as go
import plotly.express as px

# Check if httpx is available
try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False
    st.error("httpx library not found. Please install with: pip install httpx[http2]")


# Page configuration
st.set_page_config(
    page_title="Sofascore Darts Viewer",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)


class SofascoreDartsFetcher:
    """Fetches darts match data from Sofascore API using httpx with HTTP/2"""
    
    BASE_URL = "https://www.sofascore.com/api/v1"
    
    def __init__(self, delay: float = 2.0):
        self.delay = delay
        self.client = None
        self._init_client()
    
    def _init_client(self):
        """Initialize httpx client with HTTP/2 and realistic headers"""
        if not HTTPX_AVAILABLE:
            return
            
        if self.client:
            self.client.close()
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://www.sofascore.com/',
            'Origin': 'https://www.sofascore.com',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
        }
        
        self.client = httpx.Client(
            headers=headers,
            http2=True,  # Enable HTTP/2
            follow_redirects=True,
            timeout=30.0,
            verify=True
        )
    
    def _visit_homepage(self):
        """Visit homepage to establish session"""
        if not HTTPX_AVAILABLE or not self.client:
            return False
            
        try:
            response = self.client.get('https://www.sofascore.com/')
            time.sleep(random.uniform(1.5, 2.5))
            return response.status_code == 200
        except Exception as e:
            st.warning(f"Could not visit homepage: {e}")
            return False
    
    def _make_request(self, url: str, max_retries: int = 3):
        """Make a request with retry logic"""
        if not HTTPX_AVAILABLE or not self.client:
            st.error("httpx is not available. Please install: pip install httpx[http2]")
            return None
            
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    wait_time = self.delay * (2 ** attempt) + random.uniform(1, 3)
                    st.info(f"Retry {attempt + 1}/{max_retries} after {wait_time:.1f}s...")
                    time.sleep(wait_time)
                else:
                    time.sleep(self.delay + random.uniform(0.5, 1.5))
                
                response = self.client.get(url)
                
                st.info(f"HTTP Status: {response.status_code}")
                
                if response.status_code == 429:
                    retry_after = int(response.headers.get('retry-after', 60))
                    st.warning(f"Rate limited. Waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue
                
                if response.status_code == 403:
                    st.error("403 Forbidden - Access denied")
                    if attempt < max_retries - 1:
                        self._init_client()
                        self._visit_homepage()
                    continue
                
                if response.status_code == 200:
                    data = response.json()
                    st.success("✓ Successfully fetched data!")
                    # Show preview of data
                    st.json({"preview": str(data)[:200] + "..."})
                    return data
                else:
                    st.error(f"Unexpected status code: {response.status_code}")
                    response.raise_for_status()
                
            except httpx.HTTPStatusError as e:
                if attempt == max_retries - 1:
                    st.error(f"HTTP Error {e.response.status_code}")
                    return None
            except httpx.RequestError as e:
                if attempt == max_retries - 1:
                    st.error(f"Request Error: {e}")
                    return None
            except Exception as e:
                if attempt == max_retries - 1:
                    st.error(f"Error: {e}")
                    return None
        
        return None
    
    def fetch_scheduled_events(self, date: str):
        """Fetch scheduled darts events for a specific date"""
        url = f"{self.BASE_URL}/sport/darts/scheduled-events/{date}"
        result = self._make_request(url)
        
        # Debug output
        if result:
            st.info(f"✓ API returned data with {len(result.get('events', []))} events")
        else:
            st.warning("✗ API returned None or empty")
        
        return result
    
    def extract_event_ids(_self, scheduled_data):
        """Extract event IDs and basic info from scheduled events"""
        events = []
        
        if not scheduled_data or 'events' not in scheduled_data:
            return events
        
        for event in scheduled_data.get('events', []):
            # Extract nested data safely
            home_team = event.get('homeTeam', {})
            away_team = event.get('awayTeam', {})
            tournament = event.get('tournament', {})
            status = event.get('status', {})
            round_info = event.get('roundInfo', {})
            home_score = event.get('homeScore', {})
            away_score = event.get('awayScore', {})
            
            event_info = {
                'id': event.get('id'),
                'slug': event.get('slug'),
                'startTimestamp': event.get('startTimestamp'),
                'startTime': datetime.fromtimestamp(event.get('startTimestamp', 0)).strftime('%Y-%m-%d %H:%M:%S'),
                'homeTeam': home_team.get('name', 'Unknown'),
                'awayTeam': away_team.get('name', 'Unknown'),
                'tournament': tournament.get('name', 'Unknown'),
                'round': round_info.get('name', ''),
                'status': status.get('description', 'Unknown'),
                'homeScore': home_score.get('display', 0),
                'awayScore': away_score.get('display', 0),
                'bestOfSets': event.get('bestOfSets'),
                'bestOfLegs': event.get('bestOfLegs'),
                'winnerCode': event.get('winnerCode', 0)
            }
            events.append(event_info)
        
        return events
    
    def fetch_event_statistics(self, event_id: int):
        """Fetch statistics for a specific event"""
        url = f"{self.BASE_URL}/event/{event_id}/statistics"
        return self._make_request(url)
    
    def close(self):
        """Close httpx client"""
        if self.client:
            try:
                self.client.close()
            except Exception:
                pass


def display_header():
    """Display app header"""
    st.title("🎯 Sofascore Darts Data Viewer")
    st.markdown("### Fetch and analyze darts match statistics from Sofascore")
    st.markdown("---")


def display_sidebar():
    """Display sidebar with controls"""
    st.sidebar.title("⚙️ Settings")
    
    # Date selection mode
    mode = st.sidebar.radio(
        "Select Mode",
        ["Single Date", "Date Range"],
        help="Choose whether to fetch data for a single date or a range of dates"
    )
    
    if mode == "Single Date":
        date = st.sidebar.date_input(
            "Select Date",
            value=datetime.now(),
            help="Choose a date to fetch matches"
        )
        start_date = date
        end_date = date
    else:
        col1, col2 = st.sidebar.columns(2)
        with col1:
            start_date = st.sidebar.date_input(
                "Start Date",
                value=datetime.now() - timedelta(days=2),
                help="Choose start date"
            )
        with col2:
            end_date = st.sidebar.date_input(
                "End Date",
                value=datetime.now(),
                help="Choose end date"
            )
    
    st.sidebar.markdown("---")
    
    # Display options
    st.sidebar.subheader("Display Options")
    show_stats = st.sidebar.checkbox("Show Statistics", value=True)
    show_charts = st.sidebar.checkbox("Show Charts", value=True)
    auto_refresh = st.sidebar.checkbox("Auto Refresh", value=False)
    
    if auto_refresh:
        refresh_interval = st.sidebar.slider(
            "Refresh Interval (seconds)",
            min_value=30,
            max_value=300,
            value=60,
            step=30
        )
    else:
        refresh_interval = None
    
    st.sidebar.markdown("---")
    
    # Export options
    st.sidebar.subheader("Export Options")
    export_format = st.sidebar.selectbox(
        "Export Format",
        ["CSV", "JSON", "Excel"],
        help="Choose format for data export"
    )
    
    return {
        'mode': mode,
        'start_date': start_date,
        'end_date': end_date,
        'show_stats': show_stats,
        'show_charts': show_charts,
        'auto_refresh': auto_refresh,
        'refresh_interval': refresh_interval,
        'export_format': export_format
    }


def create_events_dataframe(events):
    """Create a DataFrame from events list"""
    if not events:
        return pd.DataFrame()
    
    df = pd.DataFrame(events)
    df['startTime'] = pd.to_datetime(df['startTime'])
    return df


def display_events_table(events_df):
    """Display events in a formatted table"""
    if events_df.empty:
        st.warning("No matches found for the selected date(s)")
        return
    
    st.subheader(f"📅 Found {len(events_df)} Matches")
    
    # Format the dataframe for display
    display_df = events_df[[
        'startTime', 'homeTeam', 'homeScore', 'awayScore', 'awayTeam', 
        'tournament', 'round', 'status'
    ]].copy()
    
    display_df.columns = ['Start Time', 'Home', 'Score (H)', 'Score (A)', 'Away', 'Tournament', 'Round', 'Status']
    
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True
    )


def parse_statistics(stats_data):
    """Parse statistics data into a structured format"""
    if not stats_data or 'statistics' not in stats_data:
        return None
    
    stats_dict = {}
    
    for period_data in stats_data.get('statistics', []):
        if period_data.get('period') == 'ALL':
            for group in period_data.get('groups', []):
                for item in group.get('statisticsItems', []):
                    key = item.get('key')
                    stats_dict[key] = {
                        'name': item.get('name'),
                        'home': item.get('home'),
                        'away': item.get('away'),
                        'homeValue': item.get('homeValue'),
                        'awayValue': item.get('awayValue')
                    }
    
    return stats_dict


def display_match_statistics(event, stats):
    """Display detailed statistics for a match"""
    if not stats:
        st.info("Statistics not available for this match yet")
        return
    
    st.markdown(f"### {event['homeTeam']} vs {event['awayTeam']}")
    st.markdown(f"**Tournament:** {event['tournament']} | **Time:** {event['startTime']}")
    
    # Create columns for different stat categories
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("#### 📊 Scoring")
        if 'Average3Darts' in stats:
            st.metric(
                "Avg 3 Darts (Home)",
                stats['Average3Darts']['home'],
                delta=None
            )
            st.metric(
                "Avg 3 Darts (Away)",
                stats['Average3Darts']['away'],
                delta=None
            )
        
        if 'Thrown180' in stats:
            st.metric("180s (Home)", stats['Thrown180']['home'])
            st.metric("180s (Away)", stats['Thrown180']['away'])
    
    with col2:
        st.markdown("#### 🎯 High Scores")
        if 'ThrownOver140' in stats:
            st.metric("Over 140 (Home)", stats['ThrownOver140']['home'])
            st.metric("Over 140 (Away)", stats['ThrownOver140']['away'])
        
        if 'ThrownOver100' in stats:
            st.metric("Over 100 (Home)", stats['ThrownOver100']['home'])
            st.metric("Over 100 (Away)", stats['ThrownOver100']['away'])
    
    with col3:
        st.markdown("#### 🏁 Checkouts")
        if 'HighestCheckout' in stats:
            st.metric("Highest (Home)", stats['HighestCheckout']['home'])
            st.metric("Highest (Away)", stats['HighestCheckout']['away'])
        
        if 'CheckoutsAccuracy' in stats:
            st.metric("Accuracy (Home)", stats['CheckoutsAccuracy']['home'])
            st.metric("Accuracy (Away)", stats['CheckoutsAccuracy']['away'])


def create_comparison_chart(event, stats):
    """Create comparison charts for match statistics"""
    if not stats:
        return None
    
    # Prepare data for comparison
    categories = []
    home_values = []
    away_values = []
    
    key_stats = ['Thrown180', 'ThrownOver140', 'ThrownOver100', 'CheckoutsOver100']
    
    for key in key_stats:
        if key in stats:
            categories.append(stats[key]['name'])
            try:
                home_values.append(float(stats[key]['homeValue']))
                away_values.append(float(stats[key]['awayValue']))
            except (ValueError, TypeError):
                home_values.append(0)
                away_values.append(0)
    
    if not categories:
        return None
    
    # Create grouped bar chart
    fig = go.Figure(data=[
        go.Bar(name=event['homeTeam'], x=categories, y=home_values, marker_color='#1f77b4'),
        go.Bar(name=event['awayTeam'], x=categories, y=away_values, marker_color='#ff7f0e')
    ])
    
    fig.update_layout(
        title=f"Match Statistics Comparison",
        xaxis_title="Statistic",
        yaxis_title="Count",
        barmode='group',
        height=400,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    return fig


def create_average_comparison(event, stats):
    """Create a comparison chart for average statistics"""
    if not stats or 'Average3Darts' not in stats:
        return None
    
    try:
        home_avg = float(stats['Average3Darts']['homeValue'])
        away_avg = float(stats['Average3Darts']['awayValue'])
    except (ValueError, TypeError):
        return None
    
    fig = go.Figure(go.Bar(
        x=[event['homeTeam'], event['awayTeam']],
        y=[home_avg, away_avg],
        marker_color=['#1f77b4', '#ff7f0e'],
        text=[f"{home_avg:.2f}", f"{away_avg:.2f}"],
        textposition='auto'
    ))
    
    fig.update_layout(
        title="Average 3 Darts Comparison",
        yaxis_title="Average Score",
        height=300,
        showlegend=False
    )
    
    return fig


def export_data(events_df, all_stats, format_type):
    """Export data in the selected format"""
    if events_df.empty:
        st.warning("No data to export")
        return
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if format_type == "CSV":
        csv = events_df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"darts_data_{timestamp}.csv",
            mime="text/csv"
        )
    
    elif format_type == "JSON":
        # Combine events and stats
        export_data = []
        for _, event in events_df.iterrows():
            event_dict = event.to_dict()
            event_dict['statistics'] = all_stats.get(event['id'], {})
            export_data.append(event_dict)
        
        json_str = json.dumps(export_data, indent=2, default=str)
        st.download_button(
            label="📥 Download JSON",
            data=json_str,
            file_name=f"darts_data_{timestamp}.json",
            mime="application/json"
        )
    
    elif format_type == "Excel":
        try:
            # Create Excel file in memory
            from io import BytesIO
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                events_df.to_excel(writer, sheet_name='Events', index=False)
                
                # Add statistics sheet if available
                if all_stats:
                    stats_rows = []
                    for event_id, stats in all_stats.items():
                        if stats:
                            event = events_df[events_df['id'] == event_id].iloc[0]
                            for key, value in stats.items():
                                stats_rows.append({
                                    'Event ID': event_id,
                                    'Match': f"{event['homeTeam']} vs {event['awayTeam']}",
                                    'Statistic': value['name'],
                                    'Home': value['home'],
                                    'Away': value['away']
                                })
                    
                    if stats_rows:
                        stats_df = pd.DataFrame(stats_rows)
                        stats_df.to_excel(writer, sheet_name='Statistics', index=False)
            
            excel_data = output.getvalue()
            st.download_button(
                label="📥 Download Excel",
                data=excel_data,
                file_name=f"darts_data_{timestamp}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except ImportError:
            st.error("openpyxl package required for Excel export. Install with: pip install openpyxl")


def main():
    """Main application logic"""
    
    # Check httpx availability first
    if not HTTPX_AVAILABLE:
        st.error("❌ httpx library is required but not installed!")
        st.info("""
        **To install httpx:**
        ```bash
        pip install httpx[http2]
        ```
        After installation, refresh this page.
        """)
        st.stop()
    
    # Initialize session state
    if 'fetcher' not in st.session_state:
        st.session_state.fetcher = SofascoreDartsFetcher(delay=2.0)
        # Visit homepage on initialization
        with st.spinner("Initializing session..."):
            st.session_state.fetcher._visit_homepage()
    
    if 'last_fetch_time' not in st.session_state:
        st.session_state.last_fetch_time = None
    
    # Display header
    display_header()
    
    # Display sidebar and get settings
    settings = display_sidebar()
    
    # Show connection status
    st.sidebar.success("✓ httpx HTTP/2 Ready")
    
    # Fetch button
    fetch_button = st.button("🔄 Fetch Data", type="primary", use_container_width=True)
    
    # Auto refresh logic
    if settings['auto_refresh'] and settings['refresh_interval']:
        if st.session_state.last_fetch_time:
            time_since_fetch = (datetime.now() - st.session_state.last_fetch_time).seconds
            if time_since_fetch >= settings['refresh_interval']:
                fetch_button = True
                st.rerun()
        else:
            fetch_button = True
    
    if fetch_button:
        st.session_state.last_fetch_time = datetime.now()
        
        # Fetch events
        all_events = []
        all_stats = {}
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Calculate date range
        start = settings['start_date']
        end = settings['end_date']
        date_range = pd.date_range(start, end)
        total_days = len(date_range)
        
        for idx, current_date in enumerate(date_range):
            date_str = current_date.strftime('%Y-%m-%d')
            status_text.text(f"Fetching data for {date_str}...")
            
            scheduled_data = st.session_state.fetcher.fetch_scheduled_events(date_str)
            events = st.session_state.fetcher.extract_event_ids(scheduled_data)
            
            all_events.extend(events)
            
            # Fetch statistics if option is enabled
            if settings['show_stats'] and events:
                for event in events:
                    stats_data = st.session_state.fetcher.fetch_event_statistics(event['id'])
                    if stats_data:
                        all_stats[event['id']] = parse_statistics(stats_data)
            
            progress_bar.progress((idx + 1) / total_days)
        
        status_text.text("✅ Data fetching complete!")
        time.sleep(0.5)
        status_text.empty()
        progress_bar.empty()
        
        # Store in session state
        st.session_state.events = all_events
        st.session_state.stats = all_stats
        st.session_state.events_df = create_events_dataframe(all_events)
    
    # Display data if available
    if 'events_df' in st.session_state and not st.session_state.events_df.empty:
        
        # Display events table
        display_events_table(st.session_state.events_df)
        
        st.markdown("---")
        
        # Display detailed statistics for each match
        if settings['show_stats']:
            st.subheader("📈 Match Statistics")
            
            for idx, event in enumerate(st.session_state.events):
                with st.expander(
                    f"🎯 {event['homeTeam']} vs {event['awayTeam']} - {event['tournament']}",
                    expanded=(idx == 0)  # Expand first match by default
                ):
                    stats = st.session_state.stats.get(event['id'])
                    
                    if stats:
                        display_match_statistics(event, stats)
                        
                        # Display charts
                        if settings['show_charts']:
                            st.markdown("---")
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                fig1 = create_average_comparison(event, stats)
                                if fig1:
                                    st.plotly_chart(fig1, use_container_width=True)
                            
                            with col2:
                                fig2 = create_comparison_chart(event, stats)
                                if fig2:
                                    st.plotly_chart(fig2, use_container_width=True)
                    else:
                        st.info("Statistics not available for this match")
        
        st.markdown("---")
        
        # Export section
        st.subheader("💾 Export Data")
        export_data(
            st.session_state.events_df,
            st.session_state.stats,
            settings['export_format']
        )
    
    elif fetch_button:
        st.info("No matches found for the selected date(s)")
    
    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center'>
            <p>Made with ❤️ using Streamlit & httpx | Data from Sofascore API</p>
            <p style='font-size: 0.8em; color: #666;'>Using HTTP/2 protocol for reliable data access</p>
        </div>
        """,
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
