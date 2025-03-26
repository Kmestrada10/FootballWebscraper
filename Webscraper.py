import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from tqdm import tqdm
import re

# Constants
BASE_URL = "https://fbref.com"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Focus only on La Liga (FBref ID 12)
LEAGUES = {
    "La Liga": "12"
}

def get_teams_for_league(league_id):
    """Get all teams for La Liga"""
    url = f"{BASE_URL}/en/comps/{league_id}/"
    response = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    teams = []
    # Find all team tables (current and historical)
    team_tables = soup.find_all('table', {'id': re.compile(r'results\d{4}-\d{4}\d+_overall')})
    
    for table in team_tables:
        for row in table.find_all('tr')[1:]:  # Skip header
            team_link = row.find('a', href=re.compile(r'/en/squads/'))
            if team_link:
                team_name = team_link.text
                team_url = team_link['href']
                teams.append({
                    'team_name': team_name,
                    'team_url': team_url
                })
    return teams

def get_available_seasons(team_url):
    """Get all available seasons for a team with enhanced season detection"""
    url = f"{BASE_URL}{team_url}"
    response = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    seasons = []
    
    # 1. Check previous season links in the navigation
    prev_seasons = soup.find_all('a', href=re.compile(r'/en/squads/\w+/\d{4}-\d{4}/'))
    for link in prev_seasons:
        if re.match(r'\d{4}-\d{4}', link.text.strip()):
            seasons.append({
                'season': link.text.strip(),
                'season_url': link['href']
            })
    
    # 2. Check dropdown menus for more historical seasons
    dropdowns = soup.find_all('div', class_='dropdown_menu')
    for dropdown in dropdowns:
        for link in dropdown.find_all('a', href=re.compile(r'/en/squads/\w+/\d{4}-\d{4}/')):
            if re.match(r'\d{4}-\d{4}', link.text.strip()):
                seasons.append({
                    'season': link.text.strip(),
                    'season_url': link['href']
                })
    
    # 3. Add current season
    current_season = soup.find('h1').text.strip() if soup.find('h1') else "Current"
    seasons.insert(0, {
        'season': current_season,
        'season_url': team_url
    })
    
    # Remove duplicates
    seen = set()
    unique_seasons = []
    for season in seasons:
        identifier = season['season_url']
        if identifier not in seen:
            seen.add(identifier)
            unique_seasons.append(season)
    
    return unique_seasons

def get_match_logs(season_url):
    """Get all match logs for a given season with enhanced table handling"""
    url = f"{BASE_URL}{season_url}"
    response = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    all_data = []
    
    # Find all sections with match logs
    sections = soup.find_all('div', {'class': 'section_wrapper'})
    for section in sections:
        h2 = section.find('h2')
        if h2 and 'match logs' in h2.text.lower():
            log_type = h2.text.strip()
            tables = section.find_all('table')
            
            for table in tables:
                try:
                    df = pd.read_html(str(table))[0]
                    # Clean column names if multi-index
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = ['_'.join(col).strip() for col in df.columns.values]
                    df['log_type'] = log_type
                    all_data.append(df)
                except:
                    continue
    
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

def scrape_la_liga_data():
    """Main function to scrape La Liga data"""
    all_data = []
    league_name, league_id = next(iter(LEAGUES.items()))
    
    print(f"\nScraping {league_name}...")
    teams = get_teams_for_league(league_id)
    
    for team in tqdm(teams, desc=f"Teams in {league_name}"):
        team_name = team['team_name']
        seasons = get_available_seasons(team['team_url'])
        
        for season in seasons:
            season_name = season['season']
            print(f"  Processing {team_name} - {season_name}")
            
            match_logs = get_match_logs(season['season_url'])
            if not match_logs.empty:
                match_logs['league'] = league_name
                match_logs['team'] = team_name
                match_logs['season'] = season_name
                all_data.append(match_logs)
            
            # Increased delay to be extra polite
            time.sleep(3)
    
    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

# Run the scraper
if __name__ == "__main__":
    print("Starting La Liga data collection...")
    la_liga_data = scrape_la_liga_data()
    
    if not la_liga_data.empty:
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        filename = f"la_liga_data_{timestamp}.csv"
        la_liga_data.to_csv(filename, index=False)
        print(f"Data saved to {filename}")
        print(f"Total records collected: {len(la_liga_data)}")
    else:
        print("No data was scraped.")
