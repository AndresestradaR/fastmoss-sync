"""Configuration from environment variables."""
import os
from dotenv import load_dotenv

load_dotenv()

# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

# FastMoss sync settings
FASTMOSS_REGIONS = os.getenv("FASTMOSS_REGIONS", "US,MX").split(",")
FASTMOSS_CATEGORIES = os.getenv("FASTMOSS_CATEGORIES", "14,25,9,16").split(",")
SYNC_LIMIT_PER_REGION = int(os.getenv("SYNC_LIMIT_PER_REGION", "500"))

# API settings
REQUEST_DELAY = 1.5  # Seconds between requests
MAX_RETRIES = 3
PAGE_SIZE = 50
