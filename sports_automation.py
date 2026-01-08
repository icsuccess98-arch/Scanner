import os
import time
import requests
from datetime import datetime
import pytz

BASE_URL = "http://localhost:5000"

def is_big_slate_day():
    """Check if today is Friday, Saturday, or Sunday (big slate days)."""
    et = pytz.timezone('America/New_York')
    today = datetime.now(et)
    return today.weekday() >= 4

def run_daily_automation():
    et = pytz.timezone('America/New_York')
    
    while True:
        now = datetime.now(et)
        current_hour = now.hour
        current_minute = now.minute
        is_weekend = is_big_slate_day()
        
        if current_hour == 8 and current_minute == 0:
            print(f"[{now}] Running morning fetch - games and stats...")
            try:
                requests.post(f"{BASE_URL}/fetch_games", timeout=120)
                print("Games fetched successfully")
            except Exception as e:
                print(f"Error fetching games: {e}")
        
        if current_hour == 9 and current_minute == 30:
            print(f"[{now}] Fetching odds...")
            try:
                requests.post(f"{BASE_URL}/fetch_odds", timeout=60)
                print("Odds fetched successfully")
            except Exception as e:
                print(f"Error fetching odds: {e}")
        
        if is_weekend:
            if current_hour == 10 and current_minute == 0:
                print(f"[{now}] Posting EARLY window lock...")
                try:
                    resp = requests.post(f"{BASE_URL}/post_discord_window/EARLY", timeout=30)
                    data = resp.json()
                    if data.get("success"):
                        print(f"Posted EARLY lock to Discord")
                    else:
                        print(f"EARLY post skipped: {data.get('message')}")
                except Exception as e:
                    print(f"Error posting EARLY: {e}")
            
            if current_hour == 12 and current_minute == 30:
                print(f"[{now}] Posting MID window lock...")
                try:
                    resp = requests.post(f"{BASE_URL}/post_discord_window/MID", timeout=30)
                    data = resp.json()
                    if data.get("success"):
                        print(f"Posted MID lock to Discord")
                    else:
                        print(f"MID post skipped: {data.get('message')}")
                except Exception as e:
                    print(f"Error posting MID: {e}")
            
            if current_hour == 17 and current_minute == 0:
                print(f"[{now}] Posting LATE window lock...")
                try:
                    resp = requests.post(f"{BASE_URL}/post_discord_window/LATE", timeout=30)
                    data = resp.json()
                    if data.get("success"):
                        print(f"Posted LATE lock to Discord")
                    else:
                        print(f"LATE post skipped: {data.get('message')}")
                except Exception as e:
                    print(f"Error posting LATE: {e}")
        else:
            if current_hour == 11 and current_minute == 0:
                print(f"[{now}] Posting Lock of the Day to Discord...")
                try:
                    resp = requests.post(f"{BASE_URL}/post_discord", timeout=30)
                    data = resp.json()
                    if data.get("success"):
                        print(f"Posted {data.get('picks_count', 0)} pick to Discord")
                    else:
                        print(f"Discord post skipped: {data.get('message')}")
                except Exception as e:
                    print(f"Error posting to Discord: {e}")
        
        if current_hour == 14 and current_minute == 0:
            print(f"[{now}] Afternoon refresh - updating stats and odds...")
            try:
                requests.post(f"{BASE_URL}/fetch_games", timeout=120)
                requests.post(f"{BASE_URL}/fetch_odds", timeout=60)
                print("Afternoon refresh complete")
            except Exception as e:
                print(f"Error in afternoon refresh: {e}")
        
        if current_hour == 23 and current_minute == 0:
            print(f"[{now}] Checking pick results...")
            try:
                resp = requests.post(f"{BASE_URL}/check_results", timeout=120)
                data = resp.json()
                print(f"Updated {data.get('results_updated', 0)} pick results")
            except Exception as e:
                print(f"Error checking results: {e}")
        
        time.sleep(60)

if __name__ == "__main__":
    print("Sports Automation started - running daily schedule")
    print("")
    print("WEEKDAY Schedule (Mon-Thu):")
    print("  8:00 AM  - Fetch games and stats")
    print("  9:30 AM  - Fetch odds")
    print("  11:00 AM - Post Lock of the Day")
    print("  2:00 PM  - Afternoon refresh")
    print("  11:00 PM - Check results")
    print("")
    print("WEEKEND Schedule (Fri-Sun):")
    print("  8:00 AM  - Fetch games and stats")
    print("  9:30 AM  - Fetch odds")
    print("  10:00 AM - Post EARLY Lock (before 1pm games)")
    print("  12:30 PM - Post MIDDAY Lock (1pm-6pm games)")
    print("  5:00 PM  - Post LATE Lock (6pm+ games)")
    print("  2:00 PM  - Afternoon refresh")
    print("  11:00 PM - Check results")
    print("")
    run_daily_automation()
