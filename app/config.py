import os
from dotenv import load_dotenv

load_dotenv()  # loads .env into environment

FR24_API_KEY = os.getenv("FR24_API_KEY")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

if not FR24_API_KEY:
    raise RuntimeError("FR24_API_KEY not set")

if not OPENWEATHER_API_KEY:
    raise RuntimeError("OPENWEATHER_API_KEY not set")
