import time
import random
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import os
from tqdm import tqdm

# Configuration
BASE_URL = "https://fbref.com"
LEAGUE_ID = "12"  # La Liga
DATA_FOLDER = "scraped_data"
REQUEST_DELAY = 8  # seconds between actions
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
]

def setup_driver():
    """Configure Selenium WebDriver with anti-detection settings"""
    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--headless")  # Run in background
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Disable images to load faster
    prefs = {"profile.managed_default_content_settings.images": 2}
    options.add_experimental_option("prefs", prefs)
    
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    return driver

def get_teams(driver):
    """Get all teams for La Liga"""
    url = f"{BASE_URL}/en/comps/{LEAGUE_ID}/"
    print(f"Accessing: {url}")
    
    try:
        driver.get(url)
        time.sleep(REQUEST_DELAY + random.uniform(1, 3))
        
        # Accept cookies if popup appears
        try:
            cookie_accept = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "qc-cmp2-ui"))
            cookie_accept.click()
            time.sleep(2)
        except:
            pass
        
        # Find the teams table
        table = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table.stats_table")))
        
        teams = []
        for row in table.find_elements(By.CSS_SELECTOR, "tbody tr"):
            try:
                team_link = row.find_element(By.CSS_SELECTOR, "td[data-stat='team'] a")
                teams.append({
                    "name": team_link.text,
                    "url": team_link.get_attribute("href")
                })
            except:
                continue
        
        print(f"Found {len(teams)} teams")
        return teams
    
    except Exception as e:
        print(f"Error getting teams: {str(e)}")
        return []

def get_seasons(driver, team_url):
    """Get available seasons for a team"""
    try:
        driver.get(team_url)
        time.sleep(REQUEST_DELAY + random.uniform(1, 2))
        
        seasons = []
        
        # Current season
        try:
            season_name = driver.find_element(By.TAG_NAME, "h1").text
            seasons.append({
                "name": season_name,
                "url": team_url
            })
        except:
            pass
        
        # Previous seasons
        try:
            season_links = driver.find_elements(
                By.CSS_SELECTOR, "a[href*='/squads/']")
            for link in season_links:
                if re.match(r"\d{4}-\d{4}", link.text):
                    seasons.append({
                        "name": link.text,
                        "url": link.get_attribute("href")
                    })
        except:
            pass
        
        print(f"Found {len(seasons)} seasons")
        return seasons
    
    except Exception as e:
        print(f"Error getting seasons: {str(e)}")
        return []

def get_match_logs(driver, season_url):
    """Get all match logs for a season"""
    try:
        driver.get(season_url)
        time.sleep(REQUEST_DELAY + random.uniform(1, 3))
        
        all_data = []
        
        # Find all match log sections
        sections = driver.find_elements(
            By.CSS_SELECTOR, "div.section_wrapper")
        
        for section in sections:
            try:
                title = section.find_element(By.TAG_NAME, "h2").text
                if "match log" in title.lower():
                    log_type = title
                    table = section.find_element(By.TAG_NAME, "table")
                    
                    # Get table HTML and parse with pandas
                    html = table.get_attribute("outerHTML")
                    df = pd.read_html(html)[0]
                    
                    # Clean dataframe
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = ['_'.join(col).strip() 
                                     for col in df.columns.values]
                    df['log_type'] = log_type
                    all_data.append(df)
            except:
                continue
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()
    
    except Exception as e:
        print(f"Error getting match logs: {str(e)}")
        return pd.DataFrame()

def scrape_data():
    """Main scraping function"""
    # Create data folder if not exists
    os.makedirs(DATA_FOLDER, exist_ok=True)
    
    # Initialize browser
    driver = setup_driver()
    
    try:
        # Get all teams
        teams = get_teams(driver)
        if not teams:
            print("No teams found - exiting")
            return
        
        all_data = []
        
        # Process each team
        for team in tqdm(teams, desc="Processing Teams"):
            team_name = team["name"]
            print(f"\nProcessing team: {team_name}")
            
            # Get seasons for this team
            seasons = get_seasons(driver, team["url"])
            if not seasons:
                continue
            
            # Process each season
            for season in seasons:
                season_name = season["name"]
                print(f"Processing season: {season_name}")
                
                # Get match logs
                logs = get_match_logs(driver, season["url"])
                if not logs.empty:
                    logs['team'] = team_name
                    logs['season'] = season_name
                    logs['league'] = "La Liga"
                    all_data.append(logs)
                
                # Save progress after each season
                if all_data:
                    combined = pd.concat(all_data, ignore_index=True)
                    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M")
                    filename = os.path.join(
                        DATA_FOLDER, 
                        f"la_liga_data_{timestamp}.csv")
                    combined.to_csv(filename, index=False)
                    print(f"Progress saved to {filename}")
        
        # Final save
        if all_data:
            combined = pd.concat(all_data, ignore_index=True)
            filename = os.path.join(
                DATA_FOLDER, "la_liga_final_data.csv")
            combined.to_csv(filename, index=False)
            print(f"\nFinal data saved to {filename}")
            print(f"Total records collected: {len(combined)}")
        else:
            print("\nNo data was collected")
    
    finally:
        driver.quit()

if __name__ == "__main__":
    # Check for required packages
    try:
        from selenium import webdriver
    except ImportError:
        print("Installing required packages...")
        import subprocess
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", 
            "selenium", "webdriver-manager", "pandas", "tqdm"
        ])
    
    print("Starting La Liga data collection...")
    scrape_data()