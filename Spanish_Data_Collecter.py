import requests
import pandas as pd
import time
from datetime import datetime
import os
from tqdm import tqdm

# Configuration
API_TOKEN = "7e5296414924493f9cf22386741eb283"
BASE_URL = "http://api.football-data.org/v4"
HEADERS = {"X-Auth-Token": API_TOKEN}
COMPETITION_CODE = "PD"  # La Liga
REQUEST_INTERVAL = 6.5  # 6.5s delay = ~9.2 requests/minute
DATA_FOLDER = "la_liga_full_data"
os.makedirs(DATA_FOLDER, exist_ok=True)

class LaLigaAPI:
    def __init__(self):
        self.last_request_time = 0
        
    def make_request(self, endpoint, params=None):
        """Enforce strict rate limiting"""
        time_since_last = time.time() - self.last_request_time
        if time_since_last < REQUEST_INTERVAL:
            time.sleep(REQUEST_INTERVAL - time_since_last)
            
        url = f"{BASE_URL}{endpoint}"
        response = requests.get(url, headers=HEADERS, params=params)
        self.last_request_time = time.time()
        
        if response.status_code == 403:
            print(f"[X] Paid data: {url} (skipping)")
            return None
        response.raise_for_status()
        return response.json()

api = LaLigaAPI()

def get_season_data(season_year):
    """Get all data for one season"""
    print(f"\n[SEASON] Processing {season_year}/{season_year+1}")
    
    # 1. Matches (paginated by matchday)
    matches = []
    for matchday in range(1, 39):  # La Liga has 38 matchdays
        data = api.make_request(
            f"/competitions/{COMPETITION_CODE}/matches",
            {"season": season_year, "matchday": matchday}
        )
        if data and "matches" in data:
            matches.extend(data["matches"])
    
    # 2. Standings
    standings = api.make_request(
        f"/competitions/{COMPETITION_CODE}/standings",
        {"season": season_year}
    )
    
    # 3. Scorers (top 50)
    scorers = api.make_request(
        f"/competitions/{COMPETITION_CODE}/scorers",
        {"season": season_year, "limit": 50}
    )
    
    return matches, standings, scorers

def process_data(matches, standings, scorers, season_year):
    """Convert raw API data to clean DataFrames"""
    # Process matches
    matches_df = pd.json_normalize(matches)[[
        "id", "utcDate", "matchday",
        "homeTeam.id", "homeTeam.name", 
        "awayTeam.id", "awayTeam.name",
        "score.fullTime.home", "score.fullTime.away",
        "status"
    ]].rename(columns={
        "utcDate": "date",
        "homeTeam.id": "home_team_id",
        "homeTeam.name": "home_team",
        "awayTeam.id": "away_team_id",
        "awayTeam.name": "away_team",
        "score.fullTime.home": "home_score",
        "score.fullTime.away": "away_score"
    })
    matches_df["date"] = pd.to_datetime(matches_df["date"])
    matches_df["season"] = f"{season_year}/{season_year+1}"
    
    # Process standings
    standings_df = pd.DataFrame()
    if standings and "standings" in standings:
        for table in standings["standings"]:
            if table["type"] == "TOTAL":
                df = pd.json_normalize(table["table"])
                df["season"] = f"{season_year}/{season_year+1}"
                standings_df = pd.concat([standings_df, df])
    
    # Process scorers
    scorers_df = pd.DataFrame()
    if scorers and "scorers" in scorers:
        scorers_df = pd.json_normalize(scorers["scorers"])[[
            "player.id", "player.name", "team.id", "team.name", "goals"
        ]]
        scorers_df["season"] = f"{season_year}/{season_year+1}"
    
    return matches_df, standings_df, scorers_df

def save_data(matches_df, standings_df, scorers_df, season_year):
    """Save data to season-specific files"""
    season_str = f"{season_year}_{season_year+1}"
    
    matches_df.to_csv(
        os.path.join(DATA_FOLDER, f"matches_{season_str}.csv"), 
        index=False
    )
    
    if not standings_df.empty:
        standings_df.to_csv(
            os.path.join(DATA_FOLDER, f"standings_{season_str}.csv"), 
            index=False
        )
    
    if not scorers_df.empty:
        scorers_df.to_csv(
            os.path.join(DATA_FOLDER, f"scorers_{season_str}.csv"), 
            index=False
        )
    
    print(f"[SAVED] {len(matches_df)} matches, {len(standings_df)} standings, {len(scorers_df)} scorers")

def main():
    print("[SOCCER] Collecting ALL La Liga Data (Within Rate Limits)")
    
    current_year = datetime.now().year
    for season_year in tqdm(range(2020, current_year + 1), desc="Seasons"):
        matches, standings, scorers = get_season_data(season_year)
        
        if matches is None:  # Skip if no access
            continue
            
        matches_df, standings_df, scorers_df = process_data(
            matches, standings, scorers, season_year
        )
        save_data(matches_df, standings_df, scorers_df, season_year)
        
        # Extra safety delay every 3 seasons
        if season_year % 3 == 0:
            time.sleep(10)

if __name__ == "__main__":
    start_time = time.time()
    main()
    print(f"\n[DONE] Completed in {(time.time() - start_time)/60:.1f} minutes")