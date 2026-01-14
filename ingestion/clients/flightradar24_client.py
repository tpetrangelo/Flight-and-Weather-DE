from fr24sdk.client import Client
from app.config import FR24_API_KEY

with Client(api_token=f"{FR24_API_KEY}") as client:
    result = client.live.flight_positions.get_full(airports=["US"])
    print(result)
