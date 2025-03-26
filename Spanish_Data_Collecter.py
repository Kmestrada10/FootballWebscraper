import requests
import pandas as pd
import time
from datetime import datetime
import os

# Configuration
API_TOKEN = "7e5296414924493f9cf22386741eb283"  # Your API token
BASE_URL = "http://api.football-data.org/v4"
HEADERS = {"X-Auth-Token": API_TOKEN}
COMPETITION_CODE = "PD"  # La Liga code
DATA_FOLDER = "la_liga_data"
os.makedirs(DATA_FOLDER, exist_ok=True)

def make_api_request(endpoint, params=None):
    """Make API request with error handling and rate limit control"""
    url = f"{BASE_URL}{endpoint}"
    try:
        response = requests.get(url, headers=HEADERS, params=params)
        time.sleep(6)  # Respect 10 requests/minute limit
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"API Error: {str(e)}")
        return None

def get_seasons():
    """Get all available seasons for La Liga"""
    endpoint = f"/competitions/{COMPETITION_CODE}"
    data = make_api_request(endpoint)
    return data.get("seasons", []) if data else []

def get_matches(season):
    """Get all matches for a specific season"""
    endpoint = f"/competitions/{COMPETITION_CODE}/matches"
    params = {"season": season}
    data = make_api_request(endpoint, params)
    return data.get("matches", []) if data else []

def get_standings(season):
    """Get league standings for a specific season"""
    endpoint = f"/competitions/{COMPETITION_CODE}/standings"
    params = {"season": season}
    data = make_api_request(endpoint, params)
    return data.get("standings", []) if data else []

def get_teams():
    """Get all teams in current La Liga season"""
    endpoint = f"/competitions/{COMPETITION_CODE}/teams"
    data = make_api_request(endpoint)
    return data.get("teams", []) if data else []

def get_top_scorers(season):
    """Get top scorers for a specific season"""
    endpoint = f"/competitions/{COMPETITION_CODE}/scorers"
    params = {"season": season}
    data = make_api_request(endpoint, params)
    return data.get("scorers", []) if data else []

def process_matches(matches):
    """Process matches data into DataFrame"""
    df = pd.json_normalize(matches)[[
        "utcDate", "matchday", 
        "homeTeam.name", "awayTeam.name",
        "score.fullTime.home", "score.fullTime.away",
        "status", "stage"
    ]].rename(columns={
        "utcDate": "date",
        "homeTeam.name": "home_team",
        "awayTeam.name": "away_team",
        "score.fullTime.home": "home_score",
        "score.fullTime.away": "away_score"
    })
    
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    df["result"] = df.apply(
        lambda x: "H" if x["home_score"] > x["away_score"] else 
                 "A" if x["home_score"] < x["away_score"] else "D",
        axis=1
    )
    return df

def process_standings(standings):
    """Process standings data into DataFrame"""
    tables = []
    for standing in standings:
        if standing["type"] == "TOTAL":
            df = pd.json_normalize(standing["table"])
            df["stage"] = standing["stage"]
            tables.append(df)
    return pd.concat(tables) if tables else pd.DataFrame()

def save_data(df, filename):
    """Save DataFrame to CSV"""
    path = os.path.join(DATA_FOLDER, filename)
    df.to_csv(path, index=False)
    print(f"Saved {len(df)} records to {path}")

def get_all_la_liga_data():
    """Main function to collect all La Liga data"""
    print("Starting La Liga data collection...")
    
    # 1. Get available seasons
    seasons = get_seasons()
    print(f"Found {len(seasons)} seasons")
    
    for season_data in seasons:
        season = season_data["startDate"][:4]
        print(f"\nProcessing season {season}/{int(season)+1}")
        
        # 2. Get matches
        matches = get_matches(season)
        if matches:
            matches_df = process_matches(matches)
            save_data(matches_df, f"matches_{season}_{int(season)+1}.csv")
        
        # 3. Get standings
        standings = get_standings(season)
        if standings:
            standings_df = process_standings(standings)
            save_data(standings_df, f"standings_{season}_{int(season)+1}.csv")
        
        # 4. Get top scorers (only for recent seasons)
        if int(season) >= 2020:  # Scorers not always available for older seasons
            scorers = get_top_scorers(season)
            if scorers:
                scorers_df = pd.json_normalize(scorers)
                save_data(scorers_df, f"scorers_{season}_{int(season)+1}.csv")
    
    # 5. Get current teams
    teams = get_teams()
    if teams:
        teams_df = pd.json_normalize(teams)
        save_data(teams_df, "current_teams.csv")
    
    print("\nData collection complete!")

if __name__ == "__main__":
    get_all_la_liga_data()