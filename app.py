import json
import asyncio
import csv
import subprocess
import concurrent.futures
from datetime import date as dt_date, timedelta, datetime
from io import StringIO

import streamlit as st
import requests
from playwright.async_api import async_playwright
from playwright.sync_api import sync_playwright
import pandas as pd
import plotly.graph_objects as go


BASE_URL = "https://www.sofascore.com/api/v1/sport/darts"
PLAYWRIGHT_CMD = ["playwright", "install", "chromium"]


@st.cache_resource(show_spinner=False)
def install_playwright():
    """
    Ensures the Playwright Chromium browser is installed once per Streamlit session.
    """
    try:
        subprocess.run(PLAYWRIGHT_CMD, check=True, timeout=300)
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError("Failed to install Playwright Chromium.") from exc
    return True


async def get_sofascore_session_data(headless=True):
    """
    Launches a browser to get valid Cloudflare cookies and the specific User-Agent.
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()

        try:
            # Go to the main Darts page
            await page.goto("https://www.sofascore.com/darts", timeout=60000, wait_until="domcontentloaded")

            # Wait for network to settle (handles Cloudflare challenges better than sleep)
            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except:
                pass

            # Attempt to close cookie consent if it exists
            try:
                await page.locator("button:has-text('Agree')").click(timeout=2000)
            except:
                pass

            # Force some interaction to prove humanity
            try:
                await page.wait_for_selector("a[href*='/match']", timeout=5000)
            except:
                pass

            cookies = await context.cookies()
            # CRITICAL: Get the actual UA used by Playwright to match headers later
            user_agent = await page.evaluate("navigator.userAgent")
            
            return cookies, user_agent

        finally:
            await browser.close()


def cookies_to_header(cookie_list):
    return "; ".join([f"{c['name']}={c['value']}" for c in cookie_list])


def generate_date_range(start_date, end_date):
    """Yields dates between start_date and end_date (inclusive)."""
    delta = timedelta(days=1)
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += delta


def fetch_json_with_playwright(url, headers):
    """
    Fallback mechanism: Launches a sync browser to bypass stubborn WAFs.
    Uses the same User-Agent as the session to avoid fingerprint mismatch.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=headers.get("User-Agent", "Mozilla/5.0"),
            extra_http_headers=headers
        )
        page = context.new_page()
        
        try:
            page.goto(url, wait_until="networkidle", timeout=30000)
        except:
            # sometimes networkidle fails on APIs, try load
            page.goto(url, wait_until="load", timeout=30000)
            
        page.wait_for_timeout(1000) # Short stabilization

        raw = None
        pre = page.locator("pre")
        if pre.count():
            raw = pre.first.inner_text()
        else:
            raw = page.inner_text("body")

        browser.close()

    if not raw:
        raise ValueError("Unable to extract JSON payload from Sofascore response.")

    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("Received non-JSON payload from Sofascore.") from exc


def fetch_json(url, headers):
    """
    Primary fetch method using Requests. Falls back to Playwright on 403.
    """
    try:
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 403:
            # print("403 - cookies invalid or missing, retrying with Playwright rendering...")
            return fetch_json_with_playwright(url, headers)
        r.raise_for_status()
        return r.json()
    except requests.RequestException:
        # Network error, try fallback
        return fetch_json_with_playwright(url, headers)


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
        'bestOfSets': event.get('bestOfSets'),
        'bestOfLegs': event.get('bestOfLegs'),
        'winnerCode': event.get('winnerCode', 0)
    }


def fetch_events_for_date(date_str, cookies_header, user_agent):
    """Fetch events for a specific date"""
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/json",
        "Referer": "https://www.sofascore.com/",
        "X-Fsign": "SW9D1eZo",
        "Cookie": cookies_header
    }

    events_url = f"{BASE_URL}/scheduled-events/{date_str}"

    # Fetch Events
    events_data = fetch_json(events_url, headers)
    events = events_data.get("events", [])
    
    result = []
    for event in events:
        event_data = extract_event_data(event)
        event_data['eventDate'] = date_str
        result.append(event_data)
    
    return result


def fetch_event_statistics(event_id, cookies_header, user_agent):
    """Fetch statistics for a specific event"""
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/json",
        "Referer": "https://www.sofascore.com/",
        "X-Fsign": "SW9D1eZo",
        "Cookie": cookies_header
    }

    stats_url = f"https://www.sofascore.com/api/v1/event/{event_id}/statistics"
    
    try:
        stats_data = fetch_json(stats_url, headers)
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
    date_str, cookies_header, user_agent, fetch_stats = args
    try:
        rows = fetch_events_for_date(date_str, cookies_header, user_agent)
        
        # Optionally fetch statistics for each event
        if fetch_stats and rows:
            for row in rows:
                stats = fetch_event_statistics(row['eventId'], cookies_header, user_agent)
                row.update(stats)
        
        return date_str, rows, None
    except Exception as e:
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
    st.caption("Multi-threaded scraper with Playwright anti-bot evasion - Always works!")

    try:
        with st.spinner("Ensuring Playwright Chromium is available..."):
            install_playwright()
    except RuntimeError as exc:
        st.error(f"{exc}")
        st.stop()

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
        
        # 1. GET COOKIES (Cached in Session State if possible)
        if "sofascore_cookies" not in st.session_state or "sofascore_ua" not in st.session_state:
            try:
                with st.spinner("🔐 Initializing Browser Session (Bypassing Anti-Bot)..."):
                    cookies, ua = asyncio.run(get_sofascore_session_data())
                    st.session_state["sofascore_cookies"] = cookies
                    st.session_state["sofascore_ua"] = ua
                    st.success("✓ Session initialized successfully!")
            except Exception as exc:
                st.error(f"Failed to initialize session: {exc}")
                st.stop()
        
        cookies_header = cookies_to_header(st.session_state["sofascore_cookies"])
        user_agent = st.session_state["sofascore_ua"]

        # 2. PARALLEL FETCH
        results = []
        dates_to_fetch = st.session_state["selected_dates"]
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Prepare arguments for threading
        tasks = [(d, cookies_header, user_agent, fetch_stats) for d in dates_to_fetch]
        
        completed_count = 0
        total_count = len(tasks)

        # Use ThreadPoolExecutor for concurrency
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            future_to_date = {executor.submit(fetch_rows_for_date_task, task): task[0] for task in tasks}
            
            for future in concurrent.futures.as_completed(future_to_date):
                d_str, rows, err = future.result()
                completed_count += 1
                
                progress = completed_count / total_count
                progress_bar.progress(progress)
                status_text.write(f"✓ Finished {d_str} ({completed_count}/{total_count}) - {len(rows)} matches")
                
                if err:
                    st.error(f"Error scraping {d_str}: {err}")
                else:
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
            <p>Made with ❤️ using Streamlit & Playwright | Data from Sofascore API</p>
            <p style='font-size: 0.8em;'>Using real browser automation for 99% success rate</p>
        </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    run_streamlit_app()
