import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from tqdm import tqdm
import re
import sys

# Constants
BASE_URL = "https://fbref.com"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

LEAGUES = {"La Liga": "12"}

def debug_print(message):
    """Helper function for debug output"""
    print(f"[DEBUG] {message}")

def get_teams_for_league(league_id):
    """Get all teams for La Liga with better error handling"""
    url = f"{BASE_URL}/en/comps/{league_id}/"
    debug_print(f"Fetching teams from: {url}")
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
    except Exception as e:
        debug_print(f"Failed to fetch league page: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    teams = []
    
    # Try multiple table identifiers
    table_ids = [
        'results2023-2024121_overall',  # Current season table
        'results2022-2023121_overall',  # Previous season
        'results2021-2022121_overall'   # Season before that
    ]
    
    for table_id in table_ids:
        team_table = soup.find('table', {'id': table_id})
        if team_table:
            debug_print(f"Found table with ID: {table_id}")
            for row in team_table.find_all('tr')[1:]:  # Skip header
                team_link = row.find('a', href=re.compile(r'/en/squads/'))
                if team_link:
                    team_name = team_link.text.strip()
                    team_url = team_link['href']
                    teams.append({
                        'team_name': team_name,
                        'team_url': team_url
                    })
            break
    
    if not teams:
        debug_print("No teams found in standard tables, trying alternative search")
        # Fallback: look for any squad links
        for link in soup.find_all('a', href=re.compile(r'/en/squads/\w+/')):
            if link.text.strip():
                teams.append({
                    'team_name': link.text.strip(),
                    'team_url': link['href']
                })
    
    debug_print(f"Found {len(teams)} teams")
    return teams

def get_available_seasons(team_url):
    """Get available seasons with better error handling"""
    url = f"{BASE_URL}{team_url}"
    debug_print(f"Fetching seasons from: {url}")
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
    except Exception as e:
        debug_print(f"Failed to fetch team page: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    seasons = []
    
    # Current season (from page title)
    try:
        current_season = soup.find('h1').text.strip()
        seasons.append({
            'season': current_season,
            'season_url': team_url
        })
    except:
        pass
    
    # Previous seasons from navigation
    season_links = soup.find_all('a', href=re.compile(r'/en/squads/\w+/\d{4}-\d{4}/'))
    for link in season_links:
        try:
            season_text = link.text.strip()
            if re.match(r'\d{4}-\d{4}', season_text):
                seasons.append({
                    'season': season_text,
                    'season_url': link['href']
                })
        except:
            continue
    
    debug_print(f"Found {len(seasons)} seasons")
    return seasons

def get_match_logs(season_url):
    """Get match logs with better error handling"""
    url = f"{BASE_URL}{season_url}"
    debug_print(f"Fetching match logs from: {url}")
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
    except Exception as e:
        debug_print(f"Failed to fetch season page: {e}")
        return pd.DataFrame()

    soup = BeautifulSoup(response.content, 'html.parser')
    all_data = []
    
    # Find all match log tables
    for table in soup.find_all('table', {'class': 'stats_table'}):
        try:
            df = pd.read_html(str(table))[0]
            # Add context columns
            if 'log_type' not in df.columns:
                # Try to find the section title
                prev_h2 = table.find_previous('h2')
                log_type = prev_h2.text.strip() if prev_h2 else "Unknown"
                df['log_type'] = log_type
            all_data.append(df)
        except Exception as e:
            debug_print(f"Failed to parse table: {e}")
            continue
    
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()

def scrape_la_liga_data():
    """Main scraping function with progress tracking"""
    all_data = []
    league_name, league_id = next(iter(LEAGUES.items()))
    
    print(f"\nStarting {league_name} data collection...")
    teams = get_teams_for_league(league_id)
    
    if not teams:
        print("ERROR: No teams found. Possible issues:")
        print("- Website structure changed")
        print("- Network blocking the request")
        print("- Incorrect league ID")
        return pd.DataFrame()
    
    for team in tqdm(teams, desc="Processing Teams"):
        team_name = team['team_name']
        debug_print(f"\nProcessing team: {team_name}")
        
        seasons = get_available_seasons(team['team_url'])
        if not seasons:
            debug_print(f"No seasons found for {team_name}")
            continue
        
        for season in seasons:
            season_name = season['season']
            debug_print(f"Processing season: {season_name}")
            
            match_logs = get_match_logs(season['season_url'])
            if not match_logs.empty:
                match_logs['league'] = league_name
                match_logs['team'] = team_name
                match_logs['season'] = season_name
                all_data.append(match_logs)
            
            time.sleep(3)  # Respectful delay
    
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()

if __name__ == "__main__":
    # Enable debug output
    debug_print("Starting scraper with debug output")
    
    la_liga_data = scrape_la_liga_data()
    
    if not la_liga_data.empty:
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        filename = f"la_liga_data_{timestamp}.csv"
        la_liga_data.to_csv(filename, index=False)
        print(f"\nSuccess! Data saved to {filename}")
        print(f"Total records collected: {len(la_liga_data)}")
        print("Columns available:", la_liga_data.columns.tolist())
    else:
        print("\nFailed to collect any data. Possible reasons:")
        print("1. FBref.com structure may have changed")
        print("2. Your IP might be temporarily blocked")
        print("3. Check the debug output above for clues")
        print("\nTry these solutions:")
        print("- Wait a while and try again")
        print("- Check if you can access FBref.com manually")
        print("- Update the User-Agent string in the headers")