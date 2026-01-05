import os
import time
import requests
from datetime import datetime
import pytz

BASE_URL = "http://localhost:5000"

def run_daily_automation():
    et = pytz.timezone('America/New_York')
    
    while True:
        now = datetime.now(et)
        current_hour = now.hour
        current_minute = now.minute
        
        if current_hour == 8 and current_minute == 0:
            print(f"[{now}] Running morning fetch - games and stats...")
            try:
                requests.post(f"{BASE_URL}/fetch_games", timeout=120)
                print("Games fetched successfully")
            except Exception as e:
                print(f"Error fetching games: {e}")
        
        if current_hour == 10 and current_minute == 0:
            print(f"[{now}] Fetching odds...")
            try:
                requests.post(f"{BASE_URL}/fetch_odds", timeout=60)
                print("Odds fetched successfully")
            except Exception as e:
                print(f"Error fetching odds: {e}")
        
        if current_hour == 11 and current_minute == 0:
            print(f"[{now}] Posting picks to Discord...")
            try:
                resp = requests.post(f"{BASE_URL}/post_discord", timeout=30)
                data = resp.json()
                if data.get("success"):
                    print(f"Posted {data.get('picks_count', 0)} picks to Discord")
                else:
                    print(f"Discord post skipped: {data.get('message')}")
            except Exception as e:
                print(f"Error posting to Discord: {e}")
        
        if current_hour == 14 and current_minute == 0:
            print(f"[{now}] Afternoon refresh - updating stats...")
            try:
                requests.post(f"{BASE_URL}/fetch_games", timeout=120)
                requests.post(f"{BASE_URL}/fetch_odds", timeout=60)
                print("Afternoon refresh complete")
            except Exception as e:
                print(f"Error in afternoon refresh: {e}")
        
        time.sleep(60)

if __name__ == "__main__":
    print("Sports Automation started - running daily schedule")
    print("Schedule (ET):")
    print("  8:00 AM - Fetch games and stats")
    print("  10:00 AM - Fetch odds")
    print("  11:00 AM - Post picks to Discord")
    print("  2:00 PM - Afternoon refresh")
    run_daily_automation()
