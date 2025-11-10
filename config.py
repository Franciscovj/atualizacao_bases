# Central configuration for clear_sportmonks
# You can change these values without touching code files
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Sportmonks
API_TOKEN = os.getenv('API_TOKEN')
START_DATE = '2025-11-04'
END_DATE = '2025-11-09'
INCLUDE = 'participants;league;odds;statistics'
FILTERS = 'markets:1,7,14;bookmakers:20,2'
LOCALE = 'pt'
ORDER = 'asc'
PER_PAGE = 50

# MongoDB
MONGO_URI = os.getenv('MONGO_URI')
MONGO_COLLECTION = 'fixtures'
MONGO_DB_FALLBACK = 'sportmonks_data'

# GitHub
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
GITHUB_TOKEN_BET2ALL = os.getenv('GITHUB_TOKEN_BET2ALL')
GITHUB_TOKEN_SKE = os.getenv('GITHUB_TOKEN_SKE')

