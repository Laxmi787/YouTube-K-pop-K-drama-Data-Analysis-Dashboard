import logging
import os
import time
import isodate
import pandas as pd
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
import gspread
from gspread_dataframe import set_with_dataframe
from tqdm import tqdm
import schedule

# ===== CONFIGURATION =====
CHANNEL_IDS = {
     "UC3IZKseVpdzPSBaWxBxundA": "BANGTANTV",          # BTS
     "UC9rMiEjNaCSsebs31MRDCRA": "BLACKPINK",          # BLACKPINK"
     "UCsU-I-vHLiaMfV_ceaYz5rQ": "MBCdrama",            # MBC K-Drama
     "UCaWd5_7JhbQBe4dknZhsHJg": "The Swoon",           # Netflix K-Drama (Official)
     "UCaO6TYtlC8U5ttz62hTrZgg": "JYP Entertainment",
     "UCor9DjxpV8C2DGZkK8YUI8g": "SMTOWN",
     "UC-5t4gMtr3lYCpzm6K3Gigg": "KOCOWA TV",
     "UCcQTRi69dsVYHN3exePtZ1A": "KBS Drama",



}

MAX_RESULTS = 50
API_KEY = "AIzaSyAzOeJMy2zmYh2AoKHVZgn16KEu3f1TwjY"
SHEET_NAME = "Childrens_Channels_Raw_Data"
CREDS_FILE = "C:/YouTubeScraper/credentials.json"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# ===== SERVICE INITIALIZATION =====
def init_services():
    try:
        youtube = build('youtube', 'v3', developerKey=API_KEY, static_discovery=False)
        if not os.path.exists(CREDS_FILE):
            print(f"🔴 Credentials file not found at {CREDS_FILE}")
            return youtube, None
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
        gc = gspread.authorize(creds)
        return youtube, gc
    except HttpError as e:
        print(f"🔴 YouTube API Error: {e}")
        return None, None
    except Exception as e:
        print(f"🔴 Service initialization failed: {str(e)}")
        return None, None

# ===== EXTRACT HASHTAGS =====
def extract_hashtags(description):
    return ' '.join(part for part in description.split() if part.startswith('#'))

# ===== SCRAPE YOUTUBE DATA =====
def scrape_youtube_data(youtube, channel_id, channel_name):
    all_videos = []
    next_page_token = None
    pbar = tqdm(desc=f"Scraping {channel_name}", unit="videos")

    try:
        while True:
            search_response = youtube.search().list(
                channelId=channel_id,
                part="id,snippet",
                maxResults=MAX_RESULTS,
                order="date",
                type="video",
                pageToken=next_page_token
            ).execute()

            video_ids = [item['id']['videoId'] for item in search_response.get('items', [])
                         if item['id'].get('kind') == 'youtube#video']

            if not video_ids:
                break

            videos_response = youtube.videos().list(
                id=','.join(video_ids),
                part="statistics,contentDetails,snippet",
                maxResults=MAX_RESULTS
            ).execute()

            for video in videos_response.get('items', []):
                try:
                    video_id = video['id']
                    snippet = video['snippet']
                    stats = video.get('statistics', {})
                    content_details = video['contentDetails']

                    description = snippet.get('description', '')
                    published_at_str = snippet.get('publishedAt')
                    duration_iso = content_details.get('duration')
                    duration_sec = isodate.parse_duration(duration_iso).total_seconds()

                    views = int(stats.get('viewCount', 0))
                    likes = int(stats.get('likeCount', 0))
                    comments = int(stats.get('commentCount', 0))
                    hashtags = extract_hashtags(description)

                    top_comments = []
                    pinned_comment = ''
                    try:
                        comments_response = youtube.commentThreads().list(
                            part="snippet",
                            videoId=video_id,
                            maxResults=20,
                            order="relevance",
                            textFormat="plainText"
                        ).execute()

                        comment_items = comments_response.get('items', [])
                        top_comments = [item['snippet']['topLevelComment']['snippet']['textDisplay'] for item in comment_items]
                        pinned_comment = top_comments[0] if top_comments else ''
                    except HttpError as comment_error:
                        if comment_error.resp.status == 403 and "commentsDisabled" in str(comment_error):
                            print(f"⚠️ Comments disabled for video {video_id}")
                        else:
                            print(f"⚠️ Comment fetch error for video {video_id}: {comment_error}")

                    all_videos.append({
                        'channel_id': channel_id,
                        'channel_name': channel_name,
                        'video_id': video_id,
                        'title': snippet.get('title'),
                        'published_at': published_at_str,
                        'duration_seconds': duration_sec,
                        'views': views,
                        'likes': likes,
                        'comments': comments,
                        'hashtags': hashtags,
                        'pinned_comment': pinned_comment,
                        'top_comments': top_comments,
                        'url': f"https://youtu.be/{video_id}",
                        'scraped_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'thumbnail': snippet['thumbnails']['default']['url']
                    })
                    pbar.update(1)
                except Exception as ve:
                    print(f"⚠️ Video processing error: {str(ve)}")
                    continue

            next_page_token = search_response.get('nextPageToken')
            if not next_page_token:
                break

            time.sleep(1)

    finally:
        pbar.close()

    return pd.DataFrame(all_videos)

# ===== MANAGE GOOGLE SHEET =====
def manage_google_sheet(gc, data):
    try:
        sh = gc.open(SHEET_NAME)
    except gspread.SpreadsheetNotFound:
        sh = gc.create(SHEET_NAME)
    worksheet = sh.sheet1
    worksheet.clear()
    set_with_dataframe(worksheet, data)
    return True

# ===== MAIN LOGIC =====
def main():
    print("\n🚀 Starting Children's YouTube Data Collector")
    print(f"📺 Channels: {', '.join(CHANNEL_IDS.values())}")

    youtube, gc = init_services()
    if not youtube:
        print("🔴 Cannot proceed without YouTube service")
        return

    all_data = pd.DataFrame()

    for channel_id, channel_name in CHANNEL_IDS.items():
        print(f"\n🔍 Checking {channel_name}...")
        try:
            chan_response = youtube.channels().list(
                part="snippet,statistics",
                id=channel_id
            ).execute()

            if not chan_response.get('items'):
                print(f"⚠️ Channel {channel_name} not found - check ID")
                continue

            print(f"✅ Found channel with {chan_response['items'][0]['statistics']['videoCount']} videos")

            channel_df = scrape_youtube_data(youtube, channel_id, channel_name)

            if not channel_df.empty:
                all_data = pd.concat([all_data, channel_df], ignore_index=True)
                print(f"✔ Collected {len(channel_df)} videos from {channel_name}")
            else:
                print(f"⚠️ No videos collected from {channel_name}")

        except Exception as e:
            print(f"🔴 Error processing {channel_name}: {str(e)}")
            continue

    if all_data.empty:
        print("\n🔴 No data collected from any channels")
        return

    print("\n💾 Saving data...")
    if gc:
        success = manage_google_sheet(gc, all_data)
        if success:
            print(f"\n✅ Success! Collected {len(all_data)} videos total")
            print(f"📊 Channels: {', '.join(all_data['channel_name'].unique())}")
            print(f"📅 Date range: {all_data['published_at'].min()} to {all_data['published_at'].max()}")
            print(f"\n🔗 Access your data: https://docs.google.com/spreadsheets/d/{gc.open(SHEET_NAME).id}")
    else:
        print("⚠️ Data collected but not saved (no Google Sheets connection)")
        print("Here's a sample of the data:")
        print(all_data.head())

# ===== SCHEDULING =====
if __name__ == "__main__":
    print("⏰ Starting YouTube Data Collector")
    print("First running a test collection...\n")

    main()

    print("\n⏳ Scheduling to run every 5 minutes...")
    schedule.every(5).minutes.do(main)

    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n🔴 Collector stopped by user")
