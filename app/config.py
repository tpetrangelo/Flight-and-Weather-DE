import os
from dotenv import load_dotenv

load_dotenv()  # loads .env into environment

AERODATABOX_API_KEY = os.getenv("AERODATABOX_API_KEY")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")
AWS_S3_ADB_SOURCE = os.getenv("AWS_S3_ADB_SOURCE")
AWS_S3_AIRPORTSINUSE_SOURCE = os.getenv("AWS_S3_AIRPORTSINUSE_SOURCE")
AWS_S3_OPENWEATHER_SOURCE = os.getenv("AWS_S3_OPENWEATHER_SOURCE")

if not AERODATABOX_API_KEY:
    raise RuntimeError("AERODATABOX_API_KEY not set")

if not AWS_S3_BUCKET:
    raise RuntimeError("AWS_S3_BUCKET not set")

if not AWS_S3_ADB_SOURCE:
    raise RuntimeError("AWS_S3_ADB_SOURCE not set")

if not AWS_S3_AIRPORTSINUSE_SOURCE:
    raise RuntimeError("AWS_S3_AIRPORTSINUSE_SOURCE not set")

if not AWS_S3_OPENWEATHER_SOURCE:
    raise RuntimeError("AWS_S3_OPENWEATHER_SOURCE not set")