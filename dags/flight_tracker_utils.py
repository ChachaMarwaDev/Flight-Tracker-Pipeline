"""
Shared fetch/load logic for the Flight Tracker pipeline.
Used by both the exploration notebook and the Airflow DAG.
"""

import math

import pandas as pd
import requests
from sqlalchemy import create_engine, text

OPENSKY_URL = "https://opensky-network.org/api/states/all"

DB_HOST = "postgres"       # service name from docker-compose.yml, not localhost
DB_PORT = 5432
DB_NAME = "mydatabase"
DB_USER = "root"
DB_PASSWORD = "root"

# Raw column order returned by the OpenSky /states/all endpoint
OPENSKY_COLUMNS = [
    "icao24", "callsign", "origin_country", "time_position",
    "last_contact", "longitude", "latitude", "baro_altitude",
    "on_ground", "velocity", "true_track", "vertical_rate",
    "sensors", "geo_altitude", "squawk", "spi", "position_source"
]

# One engine, reused by every task that imports this module
engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS flight_positions (
    id SERIAL PRIMARY KEY,
    icao24 VARCHAR(10),
    callsign VARCHAR(20),
    origin_country VARCHAR(100),
    time_position BIGINT,
    last_contact BIGINT,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    baro_altitude DOUBLE PRECISION,
    on_ground BOOLEAN,
    velocity DOUBLE PRECISION,
    true_track DOUBLE PRECISION,
    vertical_rate DOUBLE PRECISION,
    geo_altitude DOUBLE PRECISION,
    squawk VARCHAR(10),
    spi BOOLEAN,
    position_source INTEGER,
    ingested_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_icao24_time_position UNIQUE (icao24, time_position)
);
"""


def ensure_table() -> None:
    """Create the flight_positions table if it doesn't exist yet. Safe to call every run."""
    with engine.begin() as conn:
        conn.execute(text(CREATE_TABLE_SQL))


def fetch_states() -> pd.DataFrame:
    """Pull the current global aircraft state vector from OpenSky."""
    response = requests.get(OPENSKY_URL)
    response.raise_for_status()
    data = response.json()

    df = pd.DataFrame(data["states"], columns=OPENSKY_COLUMNS)
    df["callsign"] = df["callsign"].str.strip()
    df = df.drop(columns=["sensors"])  # always None — not in the table schema

    print(f"Fetched {len(df)} aircraft at {pd.Timestamp.now()}")
    return df


def load_states(df: pd.DataFrame) -> None:
    """Append a snapshot of aircraft states into Postgres, skipping exact duplicates."""
    records = df.to_dict(orient="records")

    # Replace NaN with None in each record — dicts can hold real None, unlike float64 columns
    for row in records:
        for key, value in row.items():
            if isinstance(value, float) and math.isnan(value):
                row[key] = None

    insert_sql = text("""
        INSERT INTO flight_positions
            (icao24, callsign, origin_country, time_position, last_contact,
             longitude, latitude, baro_altitude, on_ground, velocity,
             true_track, vertical_rate, geo_altitude, squawk, spi, position_source)
        VALUES
            (:icao24, :callsign, :origin_country, :time_position, :last_contact,
             :longitude, :latitude, :baro_altitude, :on_ground, :velocity,
             :true_track, :vertical_rate, :geo_altitude, :squawk, :spi, :position_source)
        ON CONFLICT (icao24, time_position) DO NOTHING
    """)

    with engine.begin() as conn:
        conn.execute(insert_sql, records)

    print(f"Attempted to load {len(df)} rows (duplicates skipped automatically).")