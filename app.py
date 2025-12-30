import json
import csv
import subprocess
import concurrent.futures
from datetime import date as dt_date, timedelta, datetime
from io import StringIO
import time
import random
import tempfile
import os

import streamlit as st
import pandas as pd


BASE_URL = "https://www.sofascore.com/api/v1/sport/darts"


def check_curl_available():
    """Check if curl is available"""
    try:
        result = subprocess.run(['curl', '--version'], capture_output=True, timeout=5)
        return result.returncode == 0
    except:
        return False


def generate_date_range(start_date, end_date):
    """Yields dates between start_date and end_date (inclusive)."""
    delta = timedelta(days=1)
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += delta


def fetch_json_with_curl(url, cookie_file=None, max_retries=3):
    """
    Fetch JSON using curl command - often bypasses protections better than Python libraries
    """
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                wait_time = 2 ** attempt + random.uniform(1, 2)
                time.sleep(wait_time)
            else:
                time.sleep(random.uniform(1, 2))
            
            # Build curl command
            curl_cmd = [
                'curl', '-s',
                '-H', 'Accept: */*',
                '-H', 'Accept-Language: en-US,en;q=0.9',
                '-H', 'Referer: https://www.sofascore.com/',
                '-H', 'Origin: https://www.sofascore.com',
                '-H', 'sec-fetch-dest: empty',
                '-H', 'sec-fetch-mode: cors',
                '-H', 'sec-fetch-site: same-origin',
                '-H', 'X-Fsign: SW9D1eZo',
                '-A', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                '--compressed'
            ]
            
            # Add cookie file if exists
            if cookie_file and os.path.exists(cookie_file):
                curl_cmd.extend(['-b', cookie_file, '-c', cookie_file])
            
            curl_cmd.append(url)
            
            # Execute curl
            result = subprocess.run(
                curl_cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                if attempt < max_retries - 1:
                    continue
                raise Exception(f"curl failed with code {result.returncode}")
            
            # Check for 403/404 in response
            if '403' in result.stdout or 'Forbidden' in result.stdout:
                if attempt < max_retries - 1:
                    continue
                raise Exception("403 Forbidden")
            
            if '404' in result.stdout or 'Not Found' in result.stdout:
                raise Exception("404 Not Found")
            
            # Parse JSON
            try:
                data = json.loads(result.stdout)
                return data
            except json.JSONDecodeError:
                if attempt < max_retries - 1:
                    continue
                raise Exception("Invalid JSON response")
                
        except subprocess.TimeoutExpired:
            if attempt < max_retries - 1:
                continue
            raise Exception("Request timeout")
        except Exception as e:
            if attempt == max_retries - 1:
                raise e
    
    return None


def init_session_cookies(cookie_file):
    """Initialize session by visiting homepage with curl"""
    try:
        curl_cmd = [
            'curl', '-s',
            '-c', cookie_file,
            '-A', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'https://www.sofascore.com/darts'
        ]
        
        subprocess.run(curl_cmd, capture_output=True, timeout=15)
        time.sleep(2)
        return True
    except:
        return False


def extract_event_data(event):
    """Extract relevant data from a single event"""
    home_team = event.get('homeTeam', {})
    away_team = event.get('awayTeam', {})
    tournament = event.get('tournament', {})
    status = event.get('status', {})
    round_info = event.get('roundInfo', {})
    home_score = event.get('homeScore', {})
    away_score = event.get('awayScore', {})
    
    return {
        'eventId': event.get('id'),
        'slug': event.get('slug'),
        'startTimestamp': event.get('startTimestamp'),
        'startTime': datetime.fromtimestamp(event.get('startTimestamp', 0)).strftime('%Y-%m-%d %H:%M:%S'),
        'homePlayer': home_team.get('name', 'Unknown'),
        'awayPlayer': away_team.get('name', 'Unknown'),
        'homeScore': home_score.get('display', 0),
        'awayScore': away_score.get('display', 0),
        'tournament': tournament.get('name', 'Unknown'),
        'round': round_info.get('name', ''),
        'status': status.get('description', 'Unknown'),
        'bestOfSets': event.get('bestOfSets', ''),
        'bestOfLegs': event.get('bestOfLegs', ''),
        'winnerCode': event.get('winnerCode', 0)
    }


def fetch_events_for_date(date_str, cookie_file):
    """Fetch events for a specific date using curl"""
    events_url = f"{BASE_URL}/scheduled-events/{date_str}"
    events_data = fetch_json_with_curl(events_url, cookie_file)
    
    if not events_data:
        return []
    
    events = events_data.get("events", [])
    
    result = []
    for event in events:
        event_data = extract_event_data(event)
        event_data['eventDate'] = date_str
        result.append(event_data)
    
    return result


def fetch_event_statistics(event_id, cookie_file):
    """Fetch statistics for a specific event using curl"""
    stats_url = f"https://www.sofascore.com/api/v1/event/{event_id}/statistics"
    
    try:
        stats_data = fetch_json_with_curl(stats_url, cookie_file)
        return parse_statistics(stats_data)
    except:
        return {}


def parse_statistics(stats_data):
    """Parse statistics data"""
    parsed = {}
    
    if not stats_data or 'statistics' not in stats_data:
        return parsed
    
    for period_data in stats_data.get('statistics', []):
        if period_data.get('period') == 'ALL':
            for group in period_data.get('groups', []):
                for item in group.get('statisticsItems', []):
                    key = item.get('key')
                    if key:
                        parsed[f"{key}_home"] = item.get('home', '')
                        parsed[f"{key}_away"] = item.get('away', '')
    
    return parsed


def fetch_rows_for_date_task(args):
    """Wrapper for threading that unpacks arguments"""
    date_str, fetch_stats, session_id = args
    
    # Create temporary cookie file for this thread
    cookie_file = f"/tmp/sofascore_cookies_{session_id}_{date_str}.txt"
    
    try:
        # Initialize session
        init_session_cookies(cookie_file)
        
        # Fetch events
        rows = fetch_events_for_date(date_str, cookie_file)
        
        # Optionally fetch statistics for each event
        if fetch_stats and rows:
            for row in rows:
                stats = fetch_event_statistics(row['eventId'], cookie_file)
                row.update(stats)
                time.sleep(0.5)  # Be nice to the API
        
        # Cleanup cookie file
        try:
            os.remove(cookie_file)
        except:
            pass
        
        return date_str, rows, None
    except Exception as e:
        # Cleanup cookie file on error
        try:
            os.remove(cookie_file)
        except:
            pass
        return date_str, [], str(e)


def rows_to_csv_bytes(rows):
    if not rows:
        return b""

    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")


def rows_to_json_bytes(rows):
    return json.dumps(rows, ensure_ascii=False, indent=2).encode("utf-8")


SINGLE_FILE_MODE = "Single file (combined)"
PER_DATE_MODE = "Separate file per date"


def build_filename_for_dates(dates, extension):
    if not dates:
        return f"darts_export.{extension}"

    sorted_dates = sorted(dates)
    if len(sorted_dates) == 1:
        return f"darts_{sorted_dates[0]}.{extension}"
    return f"darts_{sorted_dates[0]}_{sorted_dates[-1]}_{len(sorted_dates)}dates.{extension}"


def render_download_section(prepared_exports):
    if not prepared_exports:
        return

    fmt = prepared_exports["format"]
    mode = prepared_exports["mode"]
    results = prepared_exports["results"]

    selected_dates = [item["date"] for item in results]
    total_rows = sum(len(item["rows"]) for item in results)

    st.markdown("---")
    st.subheader("📥 Prepared Files")
    st.write(f"**{total_rows}** matches found across **{len(selected_dates)}** dates.")

    if total_rows == 0:
        st.info("No events were returned for the selected dates.")
        return

    encoder = rows_to_csv_bytes if fmt == "CSV" else rows_to_json_bytes
    extension = "csv" if fmt == "CSV" else "json"
    mime = "text/csv" if fmt == "CSV" else "application/json"

    if mode == SINGLE_FILE_MODE:
        combined = []
        for item in results:
            combined.extend(item["rows"])
        data_bytes = encoder(combined)
        filename = build_filename_for_dates(selected_dates, extension)
        
        col1, col2 = st.columns([1, 3])
        with col1:
            st.download_button(
                label=f"📥 Download {filename}",
                data=data_bytes,
                file_name=filename,
                mime=mime,
                type="primary",
                key=f"download_combined_{fmt}",
            )
        with col2:
             st.success("✓ Ready for download")
             
        with st.expander("📊 Preview Data (First 50 rows)"):
            df = pd.DataFrame(combined[:50])
            st.dataframe(df, use_container_width=True)
        return

    # Per-date files
    st.write("**Download files by date:**")
    for item in results:
        date_str = item["date"]
        rows = item["rows"]
        if not rows:
            st.info(f"{date_str}: No matches")
            continue

        filename = f"darts_{date_str}.{extension}"
        data_bytes = encoder(rows)
        
        col1, col2 = st.columns([2, 3])
        with col1:
            st.download_button(
                label=f"📥 {filename} ({len(rows)} matches)",
                data=data_bytes,
                file_name=filename,
                mime=mime,
                key=f"download_{date_str}_{fmt}",
            )
        with col2:
            with st.expander(f"Preview {date_str}"):
                df = pd.DataFrame(rows)
                st.dataframe(df, use_container_width=True)


def run_streamlit_app():
    st.set_page_config(page_title="🎯 Sofascore Darts Exporter", layout="wide")
    st.title("🎯 Sofascore Darts Data Exporter")
    st.caption("Using curl for maximum reliability - Works everywhere!")

    # Check curl availability
    if not check_curl_available():
        st.error("❌ curl is not installed or not available in PATH")
        st.info("""
        **To install curl:**
        - **Ubuntu/Debian:** `sudo apt-get install curl`
        - **macOS:** `brew install curl` (usually pre-installed)
        - **Windows:** Download from https://curl.se/windows/
        
        Note: Streamlit Cloud has curl pre-installed!
        """)
        st.stop()
    
    st.success("✓ curl is available")

    if "selected_dates" not in st.session_state:
        st.session_state["selected_dates"] = []

    # --- DATE SELECTION UI ---
    st.subheader("1️⃣ Select Dates")
    
    col_date, col_btn = st.columns([2, 1])
    with col_date:
        date_selection = st.date_input(
            "Pick a date OR a range (click start, then end)",
            value=[],
            min_value=dt_date(2020, 1, 1),
            max_value=dt_date.today() + timedelta(days=30),
            format="YYYY-MM-DD",
            help="To select a range: click the first date, then click the last date."
        )

    with col_btn:
        st.write("") # Spacing
        st.write("") 
        if st.button("➕ Add to Queue", use_container_width=True):
            new_dates = []
            if len(date_selection) == 2:
                start, end = date_selection
                if start > end:
                    st.error("Start date must be before end date.")
                else:
                    for d in generate_date_range(start, end):
                        new_dates.append(d.strftime("%Y-%m-%d"))
                    st.toast(f"Added {len(new_dates)} dates to queue.", icon="✅")
            elif len(date_selection) == 1:
                new_dates.append(date_selection[0].strftime("%Y-%m-%d"))
                st.toast("Added 1 date to queue.", icon="✅")
            else:
                st.warning("Please pick a date or range first.")

            if new_dates:
                current_set = set(st.session_state["selected_dates"])
                for d_str in new_dates:
                    if d_str not in current_set:
                        st.session_state["selected_dates"].append(d_str)
                st.session_state["selected_dates"].sort()

    # --- QUEUE DISPLAY ---
    if st.session_state["selected_dates"]:
        st.markdown(f"**📋 Queue:** `{len(st.session_state['selected_dates'])} dates selected`")
        with st.expander("View/Edit Queue"):
            st.write(", ".join(st.session_state["selected_dates"]))
            if st.button("🗑️ Clear Queue"):
                st.session_state["selected_dates"] = []
                st.session_state.pop("prepared_exports", None)
                st.rerun()
    else:
        st.info("Queue is empty. Add dates to proceed.")

    st.divider()

    # --- EXPORT SETTINGS ---
    st.subheader("2️⃣ Export Settings")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        export_format = st.radio("📄 Format", ["CSV", "JSON"], horizontal=True)
    with col2:
        export_mode = st.radio("📦 Mode", [SINGLE_FILE_MODE, PER_DATE_MODE], horizontal=True)
    with col3:
        fetch_stats = st.checkbox("📊 Include Statistics", value=False, 
                                   help="Fetch detailed statistics for each match (slower)")

    if st.button("🚀 Start Scraping", type="primary", disabled=not st.session_state["selected_dates"]):
        
        # Generate unique session ID for this scraping session
        session_id = int(time.time())
        
        results = []
        dates_to_fetch = st.session_state["selected_dates"]
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Prepare arguments for threading
        tasks = [(d, fetch_stats, session_id) for d in dates_to_fetch]
        
        completed_count = 0
        total_count = len(tasks)

        # Use ThreadPoolExecutor for concurrency
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_to_date = {executor.submit(fetch_rows_for_date_task, task): task[0] for task in tasks}
            
            for future in concurrent.futures.as_completed(future_to_date):
                d_str, rows, err = future.result()
                completed_count += 1
                
                progress = completed_count / total_count
                progress_bar.progress(progress)
                
                if err:
                    status_text.write(f"✗ Error {d_str} ({completed_count}/{total_count}): {err}")
                    st.error(f"Error scraping {d_str}: {err}")
                else:
                    status_text.write(f"✓ Finished {d_str} ({completed_count}/{total_count}) - {len(rows)} matches")
                    results.append({"date": d_str, "rows": rows})

        # Sort results by date
        results.sort(key=lambda x: x["date"])

        st.session_state["prepared_exports"] = {
            "format": export_format,
            "mode": export_mode,
            "results": results,
        }
        
        status_text.success("🎉 Scraping Complete!")
        progress_bar.empty()

    # --- DOWNLOAD SECTION ---
    prepared_exports = st.session_state.get("prepared_exports")
    render_download_section(prepared_exports)

    # Footer
    st.markdown("---")
    st.markdown("""
        <div style='text-align: center; color: #666;'>
            <p>Made with ❤️ using Streamlit & curl | Data from Sofascore API</p>
            <p style='font-size: 0.8em;'>Using native curl for maximum compatibility</p>
        </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    run_streamlit_app()
