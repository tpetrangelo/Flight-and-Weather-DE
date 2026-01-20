import airportsdata

# Load once at the module level so it stays in memory
AIRPORTS = airportsdata.load('IATA')

def get_coords(iata_code):
    """Returns (lat, lon) or (None, None) if not found."""
    airport = AIRPORTS.get(iata_code)
    if airport:
        return airport['lat'], airport['lon']
    return None, None