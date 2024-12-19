import os
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from typing import List
import isodate 

class YouTubeDataFetcher:
    def __init__(self, api_key):
        """
        Initialize the YouTube Data Fetcher.
        
        Args:
            api_key: YouTube Data API key for authentication
        """
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.api_key = api_key
        # YouTube category IDs (most common ones)
        self.category_ids = {
            'film_animation': 1,
            'autos': 2,
            'music': 10,
            'pets_animals': 15,
            'sports': 17,
            'gaming': 20,
            'blogging': 22,
            'comedy': 23,
            'entertainment': 24,
            'news_politics': 25,
            'howto_style': 26,
            'education': 27,
            'science_tech': 28,
            'nonprofits': 29
        }

    def get_channel_id(self, channel_name):
        """
        Find the channel ID for a given channel name.
        
        Args:
            channel_name: Name of the YouTube channel
            
        Returns:
            str: Channel ID if found, None otherwise
        """
        try:
            request = self.youtube.search().list(
                part='id,snippet',
                q=channel_name,
                type='channel',
                maxResults=1
            )
            response = request.execute()
            
            if response['items']:
                return response['items'][0]['id']['channelId']
            return None
        except Exception as e:
            print(f"Error finding channel ID: {e}")
            return None

    def get_video_category(self, video_id):
        """
        Get category information for a specific video.
        
        Args:
            video_id: YouTube video ID
            
        Returns:
            str: Category ID if found, None otherwise
        """
        try:
            request = self.youtube.videos().list(
                part='snippet',
                id=video_id
            )
            response = request.execute()
            if response['items']:
                return response['items'][0]['snippet']['categoryId']
            return None
        except Exception as e:
            print(f"Error getting video category: {e}")
            return None

    def fetch_channel_videos(self, 
                           channel_id, 
                           start_date=None, 
                           end_date=None, 
                           max_videos=50,
                           category=None):
        """
        Fetch videos from a specific channel with optional filters.
        
        Args:
            channel_id: YouTube channel ID
            start_date: Start date for video search (default: 1 year ago)
            end_date: End date for video search (default: current date)
            max_videos: Maximum number of videos to fetch (default: 50)
            category: Filter videos by category name (default: None)
            
        Returns:
            pandas.DataFrame: DataFrame containing video details
        """
        # Set default date range if not provided
        if not start_date:
            start_date = datetime.now() - timedelta(days=365)
        if not end_date:
            end_date = datetime.now()

        # Convert dates to RFC 3339 format
        published_after = start_date.isoformat() + 'Z'
        published_before = end_date.isoformat() + 'Z'

        video_details = []
        next_page_token = None

        # Get category ID if category is specified
        target_category_id = str(self.category_ids.get(category)) if category else None
        
        # Get channel details including subscriber count
        try:
            channel_response = self.youtube.channels().list(
                part='statistics',
                id=channel_id
            ).execute()
            subscriber_count = int(channel_response['items'][0]['statistics']['subscriberCount'])
        except Exception as e:
            print(f"Error fetching channel statistics: {e}")
            subscriber_count = 0

        # Reverse mapping of category IDs to names
        category_names = {str(v): k for k, v in self.category_ids.items()}

        try:
            while len(video_details) < max_videos:
                # Search for videos in the channel
                request = self.youtube.search().list(
                    part='id,snippet',
                    channelId=channel_id,
                    type='video',
                    order='date',
                    maxResults=min(50, max_videos - len(video_details)),
                    publishedAfter=published_after,
                    publishedBefore=published_before,
                    pageToken=next_page_token
                )
                response = request.execute()

                # Fetch detailed video statistics
                for item in response.get('items', []):
                    video_id = item['id']['videoId']
                    
                    # Get video statistics and category
                    stats_request = self.youtube.videos().list(
                        part='statistics,snippet,contentDetails',  # Added contentDetails
                        id=video_id
                    )
                    stats_response = stats_request.execute()
                    
                    if stats_response['items']:
                        video_info = stats_response['items'][0]
                        video_category_id = video_info['snippet']['categoryId']
                        
                        # Skip if category doesn't match (when category filter is active)
                        if target_category_id and video_category_id != target_category_id:
                            continue
                        
                        # Get duration in seconds
                        duration_str = video_info['contentDetails']['duration']  # Format: PT#M#S
                        duration_seconds = self._parse_duration(duration_str)
                        
                        # Get comment details if available
                        try:
                            comments_response = self.youtube.commentThreads().list(
                                part='snippet',
                                videoId=video_id,
                                maxResults=1
                            ).execute()
                            has_comments_enabled = True
                        except:
                            has_comments_enabled = False
                        
                        video_details.append({
                            'video_id': video_id,
                            'title': item['snippet']['title'],
                            'description': item['snippet']['description'],
                            'published_at': item['snippet']['publishedAt'],
                            'thumbnail_url': item['snippet']['thumbnails']['high']['url'],
                            'views': int(video_info['statistics'].get('viewCount', 0)),
                            'likes': int(video_info['statistics'].get('likeCount', 0)),
                            'comments_count': int(video_info['statistics'].get('commentCount', 0)),
                            'current_subscriber_count': subscriber_count,
                            'duration_seconds': duration_seconds,
                            'comments_enabled': has_comments_enabled,
                            'category_id': video_category_id,
                            'category_name': category_names.get(video_category_id, 'Unknown'),
                            'tags': video_info['snippet'].get('tags', []),
                            'default_language': video_info['snippet'].get('defaultLanguage', 'Unknown'),
                            'default_audio_language': video_info['snippet'].get('defaultAudioLanguage', 'Unknown')
                        })

                        if len(video_details) >= max_videos:
                            break

                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break

        except Exception as e:
            print(f"Error fetching videos: {e}")

        return pd.DataFrame(video_details)

    def _parse_duration(self, duration_str: str) -> int:
        """
        Convert YouTube duration string (PT#M#S) to seconds.
        
        Args:
            duration_str: Duration string in ISO 8601 format
            
        Returns:
            int: Duration in seconds
        """
        try:
            return int(isodate.parse_duration(duration_str).total_seconds())
        except:
            return 0

    def save_to_csv(self, dataframe, filename):
        """
        Save DataFrame to CSV file.
        
        Args:
            dataframe: Pandas DataFrame to save
            filename: Output CSV filename path
        """
        try:
            dataframe.to_csv(filename, index=False)
            print(f"Data saved to {filename}")
        except Exception as e:
            print(f"Error saving to CSV: {e}")

    def get_guide_categories(self, part='snippet', region_code='US'):
        """
        Retrieve available guide categories for a specific region.
        
        Args:
            part: API response parts to include (default: 'snippet')
            region_code: Two-letter country code (default: 'US')
            
        Returns:
            dict: Dictionary mapping category IDs to category names
        """
        request = self.youtube.videoCategories().list(
            part=part,
            regionCode=region_code
        )
        response = request.execute()
        
        categories = {}
        for category in response.get('items', []):
            categories[category['id']] = category['snippet']['title']
        
        return categories

    def search_channels_by_category(self, category_id, max_results=50):
        """
        Search for channels within a specific guide category.
        
        Args:
            category_id: YouTube category ID
            max_results: Maximum number of channels to return (default: 50)
            
        Returns:
            list: List of dictionaries containing channel information
        """
        request = self.youtube.search().list(
            part='snippet',
            type='channel',
            regionCode='US',
            videoCategoryId=category_id,
            maxResults=max_results
        )
        
        response = request.execute()
        
        channels = []
        for item in response.get('items', []):
            channels.append({
                'channel_id': item['snippet']['channelId'],
                'channel_title': item['snippet']['title'],
                'description': item['snippet']['description']
            })
        
        return channels


    def get_top_channels_by_category(category_id: int, api_key: str, sample_size: int = 1000) -> List[dict]:
        """
        Fetch top YouTube channels for a category and sample based on normal distribution.
        
        Args:
            category_id: YouTube category ID
            api_key: YouTube Data API key
            sample_size: Number of channels to return (default: 1000)
            
        Returns:
            List[dict]: List of channel data dictionaries
        """
        # Initialize YouTube API client
        youtube = build("youtube", "v3", developerKey=api_key)
        
        channels = []
        next_page_token = None
        
        # Fetch channels until we have enough data
        while len(channels) < sample_size * 2:  # Get more than needed for sampling
            try:
                request = youtube.search().list(
                    part="snippet",
                    maxResults=50,
                    type="channel",
                    videoCategoryId=str(category_id),
                    order="viewCount",
                    pageToken=next_page_token
                )
                response = request.execute()
                
                # Get detailed channel info including subscriber counts
                channel_ids = [item['snippet']['channelId'] for item in response['items']]
                channel_request = youtube.channels().list(
                    part="statistics,snippet",
                    id=','.join(channel_ids)
                )
                channel_response = channel_request.execute()
                
                for channel in channel_response['items']:
                    channels.append({
                        'channel_id': channel['id'],
                        'title': channel['snippet']['title'],
                        'subscriber_count': int(channel['statistics']['subscriberCount']),
                        'video_count': int(channel['statistics']['videoCount']),
                        'view_count': int(channel['statistics']['viewCount'])
                    })
                    
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
                    
            except Exception as e:
                print(f"Error fetching channels: {e}")
                break
        
        # Sort by subscriber count
        channels.sort(key=lambda x: x['subscriber_count'], reverse=True)
        
        # Sample using normal distribution
        if len(channels) > sample_size:
            # Generate indices using normal distribution
            mu = 0  # Mean at the start (most subscribers)
            sigma = len(channels) / 4  # Standard deviation
            indices = np.random.normal(mu, sigma, sample_size)
            indices = np.clip(indices, 0, len(channels)-1).astype(int)
            indices = np.unique(indices)  # Remove duplicates
            
            # Get channels at those indices
            sampled_channels = [channels[i] for i in sorted(indices)]
            return sampled_channels[:sample_size]
        
        return channels