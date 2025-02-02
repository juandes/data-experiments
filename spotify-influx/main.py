#!/usr/bin/env python3

import os
import requests
from datetime import datetime, timedelta, timezone
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
SPOTIFY_CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID", "<YOUR_CLIENT_ID>")
SPOTIFY_CLIENT_SECRET = os.environ.get(
    "SPOTIFY_CLIENT_SECRET", "<YOUR_CLIENT_SECRET>")
SPOTIFY_REFRESH_TOKEN = os.environ.get(
    "SPOTIFY_REFRESH_TOKEN", "<YOUR_REFRESH_TOKEN>")

INFLUXDB_URL = os.environ.get("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.environ.get("INFLUXDB_TOKEN", "<YOUR_INFLUXDB_TOKEN>")
INFLUXDB_ORG = os.environ.get("INFLUXDB_ORG", "<YOUR_ORG>")
INFLUXDB_BUCKET = os.environ.get("INFLUXDB_BUCKET", "spotify")


def get_spotify_access_token():
    """
    Exchanges a Spotify refresh token for a new access token.
    """
    url = "https://accounts.spotify.com/api/token"
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": SPOTIFY_REFRESH_TOKEN,
        "client_id": SPOTIFY_CLIENT_ID,
        "client_secret": SPOTIFY_CLIENT_SECRET
    }

    response = requests.post(url, data=payload)
    response.raise_for_status()
    data = response.json()
    return data["access_token"]


def fetch_recently_played(access_token, after_ts_ms):
    """
    Calls Spotify's 'recently-played' endpoint with 'after' param in ms.
    Returns the JSON response with recent tracks.
    """
    url = "https://api.spotify.com/v1/me/player/recently-played"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    params = {
        "limit": 50,
        "after": after_ts_ms
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def write_to_influxdb(client, data):
    """
    Write the fetched Spotify data to InfluxDB using the v2 Python client.
    """
    points = []
    for item in data.get("items", []):
        played_at_str = item["played_at"]  # e.g. "2025-01-23T19:14:26.162Z".
        track = item["track"]
        played_at_dt = datetime.fromisoformat(
            played_at_str.replace("Z", "+00:00"))

        track_name = track.get("name", "Unknown")
        duration_ms = track.get("duration_ms", 0)
        is_explicit = track.get("explicit", False)
        track_id = track.get("id", "Unknown")

        # Main artist (first in list).
        if track.get("artists"):
            main_artist_name = track["artists"][0].get("name", "Unknown")
            main_artist_id = track["artists"][0].get("id", "Unknown")

        album_name = track.get("album", {}).get("name", "Unknown")

        point = (
            Point("spotify_plays")
            .time(played_at_dt, WritePrecision.NS)
            .field("track_name", track_name)
            .field("duration_ms", duration_ms)
            .field("explicit", int(is_explicit))  # store bool as int
            .field("track_id", track_id)
            .field("main_artist_name", main_artist_name)
            .field("main_artist_id", main_artist_id)
            .field("album_name", album_name)
        )
        points.append(point)

    if points:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        write_api.write(INFLUXDB_BUCKET, INFLUXDB_ORG, points)


def main():
    access_token = get_spotify_access_token()

    # Calculate "after" timestamp in ms for 1 hour ago.
    one_hour_ago = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    after_ts_ms = int(one_hour_ago.timestamp() * 1000)
    print(f"Fetching recently played after (1 hour ago): {after_ts_ms}")

    # I'm not adding pagination because I don't expect to have played than 50 songs in an hour.
    data = fetch_recently_played(access_token, after_ts_ms)

    print("Writing data to InfluxDB...")
    with InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG) as client:
        write_to_influxdb(client, data)

    print("Done.")


if __name__ == "__main__":
    main()
