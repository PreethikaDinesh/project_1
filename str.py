import googleapiclient.discovery
api_key="AIzaSyDwAlsCnEjP-HAomXIbGwPQtD6cCqI5Zic"
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
import pymongo
import streamlit as st
from googleapiclient.errors import HttpError
from datetime import datetime
from pymongo import errors
import pandas as pd
import mysql.connector
con_obj = pymongo.MongoClient("mongodb://preethi:guvi2024@ac-40kiqlq-shard-00-00.8zhqhxd.mongodb.net:27017,ac-40kiqlq-shard-00-01.8zhqhxd.mongodb.net:27017,ac-40kiqlq-shard-00-02.8zhqhxd.mongodb.net:27017/?ssl=true&replicaSet=atlas-3eyd5j-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster0")
youtube_db = con_obj["preethika"]
youtube_cdata=youtube_db["Channels"]
youtube_vdata = youtube_db["videos"]
youtube_codata = youtube_db["comments"]


mysql_conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="dataset"
)
cursor = mysql_conn.cursor()

# Function to create MySQL tables
def create_tables():
    create_channel_table()
    create_video_table()
    create_comment_table()

# Function to create channel data table
def create_channel_table():
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS channeldata(
                            Channel_Name VARCHAR(100),
                            Channel_Id VARCHAR(80) PRIMARY KEY,
                            Subscribers BIGINT,
                            Views BIGINT,
                            Total_Videos INT,
                            Channel_Description TEXT,
                            Playlist_Id VARCHAR(80)
                         )'''
        cursor.execute(create_query)
        mysql_conn.commit()
        st.success("Table 'channeldata' created successfully!")
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")

# Function to create video data table
def create_video_table():
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS videos (
                            Video_Id VARCHAR(80) PRIMARY KEY,
                            Video_Name VARCHAR(255),
                            Description TEXT,
                            Channel_Name VARCHAR(100),
                            Channel_Id VARCHAR(80),
                            Tags TEXT,
                            Thumbnail VARCHAR(255),
                            Published_Date DATETIME,
                            Duration VARCHAR(20),
                            Views INT,
                            Likes INT,
                            Comments INT,
                            Favorite_Count INT,
                            Definition VARCHAR(20),
                            Caption_Status VARCHAR(20)
                        )'''
        cursor.execute(create_query)
        mysql_conn.commit()
        st.success("Table 'videos' created successfully!")
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")

# Function to create comment data table
def create_comment_table():
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS video_comments (
                            Comment_Id VARCHAR(80) PRIMARY KEY,
                            Video_Id VARCHAR(80),
                            Comment_Text TEXT,
                            Comment_Author VARCHAR(100),
                            Comment_Published DATETIME,
                            FOREIGN KEY (Video_Id) REFERENCES videos(Video_Id)
                        )'''
        cursor.execute(create_query)
        mysql_conn.commit()
        st.success("Table 'video_comments' created successfully!")
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")

# Function to get channel data from YouTube API
def get_channel_data(channel_id):
    try:
        #youtube = googleapiclient.discovery.build("youtube", "v3", developerKey='api_key')
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
        response = request.execute()
        channel_data = {
            "Channel_Name": response['items'][0]["snippet"]["title"],
            "Channel_Id": response['items'][0]["id"],
            "Subscribers": response['items'][0]['statistics']['subscriberCount'],
            "Views": response['items'][0]["statistics"]["viewCount"],
            "Total_Videos": response['items'][0]["statistics"]["videoCount"],
            "Channel_Description": response['items'][0]["snippet"]["description"],
            "Playlist_Id": response['items'][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        }
        return channel_data
    except HttpError as e:
        st.error(f"HttpError: {e}")
        return None
def get_uploads_playlist_id(channel_id):
    try:
        request = youtube.channels().list(
            part="contentDetails",
            id=channel_id
        )
        response = request.execute()
        playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        return playlist_id
    except HttpError as e:
        print(f"HttpError: {e}")
        # Handle the error gracefully, e.g., by returning a default value or raising a custom exception
        return None

# Function to get video IDs from a playlist
def get_video_ids_from_playlist(playlist_id, max_results=100):
    video_ids = []
    try:
        #youtube = googleapiclient.discovery.build("youtube", "v3", developerKey='api_key')
        next_page_token = None
        while len(video_ids) < max_results:
            request = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=min(50, max_results - len(video_ids)),
               pageToken=next_page_token
            )
            response = request.execute()
            for item in response['items']:
                video_ids.append(item['contentDetails']['videoId'])
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
    except HttpError as e:
        st.error(f"HttpError: {e}")
    return video_ids

# Function to get video data from YouTube API
def get_video_data(video_id):
    try:
        #youtube = googleapiclient.discovery.build("youtube", "v3", developerKey='api_key')
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()
        video_info = response['items'][0]

        published_datetime_str = video_info['snippet']['publishedAt']
        published_datetime = datetime.strptime(published_datetime_str, '%Y-%m-%dT%H:%M:%SZ')
        published_datetime_mysql_format = published_datetime.strftime('%Y-%m-%d %H:%M:%S')

        video_data = {
            'Video_Id': video_id,
            'Video_Name': video_info['snippet']['title'],
            'Description': video_info['snippet']['description'],
            'Channel_Name': video_info['snippet']['channelTitle'],
            'Channel_Id': video_info['snippet']['channelId'],
            'Tags': ','.join(video_info['snippet'].get('tags', [])),
            'Thumbnail': video_info['snippet']['thumbnails']['default']['url'],
            'Published_Date': published_datetime_mysql_format,
            'Duration': video_info['contentDetails'].get('duration', ''),
            'Views': int(video_info['statistics'].get('viewCount', 0)),
            'Likes': int(video_info['statistics'].get('likeCount', 0)),
            'Comments': int(video_info['statistics'].get('commentCount', 0)),
            'Favorite_Count': int(video_info['statistics'].get('favoriteCount', 0)),
            'Definition': video_info['contentDetails'].get('definition', ''),
            'Caption_Status': video_info['contentDetails'].get('caption', '')
        }
        return video_data
    except HttpError as e:
        st.error(f"HttpError: {e}")
        return None
def get_video_comments(video_id, max_results=100):
    comments = []
    try:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=min(100, max_results),
        )
        response = request.execute()
        for item in response['items']:
            comment_snippet = item['snippet']['topLevelComment']['snippet']
            # Convert published datetime string to MySQL format
            published_datetime_str = comment_snippet['publishedAt']
            published_datetime = datetime.strptime(published_datetime_str, '%Y-%m-%dT%H:%M:%SZ')
            published_datetime_mysql_format = published_datetime.strftime('%Y-%m-%d %H:%M:%S')

            comments.append({
                'Comment_Id': item['snippet']['topLevelComment']['id'],
                'Video_Id': video_id,
                'Comment_Text': comment_snippet['textDisplay'],
                'Comment_Author': comment_snippet['authorDisplayName'],
                'Comment_Published': published_datetime_mysql_format
            })
    except HttpError as e:
        print(f"HttpError: {e}")
    return comments

# Function to fetch and insert data into MySQL and MongoDB
def fetch_and_insert_data(channel_id):
    create_tables()
    channel_data = get_channel_data(channel_id)
    if channel_data:
        try:
            # Check if the channel already exists in the database
            cursor.execute("SELECT Channel_Id FROM channeldata WHERE Channel_Id = %s", (channel_data['Channel_Id'],))
            existing_channel = cursor.fetchone()
            if existing_channel:
                st.warning("Channel data already exists in the database.")
            else:
                # Insert the channel data into MySQL
                cursor.execute("""INSERT INTO channeldata
                                  (Channel_Name, Channel_Id, Subscribers, Views, Total_Videos, Channel_Description, Playlist_Id)
                                  VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                               (channel_data['Channel_Name'], channel_data['Channel_Id'], channel_data['Subscribers'],
                                channel_data['Views'], channel_data['Total_Videos'], channel_data['Channel_Description'],
                                channel_data['Playlist_Id']))
                mysql_conn.commit()
                st.success("Channel data inserted successfully into MySQL!")
                
                # Insert the channel data into MongoDB
                youtube_cdata.insert_one(channel_data)
        except mysql.connector.Error as err:
            st.error(f"Error: {err}")

        uploads_playlist_id = channel_data["Playlist_Id"]
        video_ids = get_video_ids_from_playlist(uploads_playlist_id, max_results=50)

        for video_id in video_ids:
            video_data = get_video_data(video_id)
            if video_data:
                try:
                    # Insert the video data into MySQL
                    cursor.execute("""INSERT INTO videos (Video_Id, Video_Name, Description, Channel_Name,
                                                        Channel_Id, Tags, Thumbnail, Published_Date,
                                                        Duration, Views, Likes,Comments, Favorite_Count,
                                                        Definition, Caption_Status)
                                                        VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                                   (video_data['Video_Id'], video_data['Video_Name'], video_data['Description'],
                                    video_data['Channel_Name'], video_data['Channel_Id'], video_data['Tags'],
                                    video_data['Thumbnail'], video_data['Published_Date'], video_data['Duration'],
                                    video_data['Views'], video_data['Likes'],video_data['Comments'],
                                    video_data['Favorite_Count'], video_data['Definition'],
                                    video_data['Caption_Status']))
                    mysql_conn.commit()
                    #st.success(f"Video data inserted for Video ID: {video_data['Video_Id']} into MySQL!")

                    # Insert the video data into MongoDB
                    youtube_vdata.insert_one(video_data)
                except mysql.connector.Error as err:
                    st.error(f"Error: {err}")

                # Get and insert video comments
                video_comments = get_video_comments(video_id)
                if video_comments:
                    try:
                        for comment in video_comments:
                            cursor.execute("""INSERT INTO video_comments (Comment_Id, Video_Id, Comment_Text,
                                                                            Comment_Author, Comment_Published)
                                                                            VALUES (%s, %s, %s, %s, %s)""",
                                           (comment['Comment_Id'], comment['Video_Id'], comment['Comment_Text'],
                                            comment['Comment_Author'], comment['Comment_Published']))
                        mysql_conn.commit()
                        #st.success(f"{len(video_comments)} comments inserted for Video ID: {video_id} into MySQL!")

                        youtube_codata.insert_many(video_comments)
                    except mysql.connector.Error as err:
                        st.error(f"Error: {err}")
                else:
                    st.warning(f"No comments found for Video ID: {video_id}")
            else:
                st.warning(f"No data found for Video ID: {video_id}")
    else:
        st.warning("No channel data retrieved.")

# Streamlit UI
st.markdown("<h1 style='text-align: center;'>YouTube Data Harvest</h1>", unsafe_allow_html=True)
#st.title("YouTube Data Harvest")
#st.title('_Streamlit_ is :blue[cool] :sunglasses:')

# Sidebar for input
st.sidebar.header("Enter Channel ID")
channel_id = st.sidebar.text_input("Channel ID")

# Main content
if st.sidebar.button("Fetch and Insert Data"):
    st.write("Fetching data for Channel ID:", channel_id)
    fetch_and_insert_data(channel_id)
# Streamlit UI
st.subheader("Display SQL Table and Mangodb in Streamlit")
# Function to fetch channel data from MySQL
def fetch_channeldata():
    try:
        cursor.execute("SELECT * FROM channeldata")
        rows = cursor.fetchall()
        return rows
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")


# Function to fetch video data from MySQL
def fetch_video_data():
    try:
        cursor.execute("SELECT * FROM videos")
        rows = cursor.fetchall()
        return rows
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")

# Function to fetch comment data from MySQL
def fetch_comment_data():
    try:
        cursor.execute("SELECT * FROM video_comments")
        rows = cursor.fetchall()
        return rows
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")

# Streamlit UI
#st.title("Display SQL Table in Streamlit")
 # Radio button to select table
select_option = st.radio("select option", ("", "sql table", "mangodb data"))

if select_option == "sql table":
    selected_table = st.radio("Select Table", ("", "channeldata", "Video Data", "Comment Data"))

    # Display selected table
    if selected_table == "channeldata":
        st.header("channeldata")
        channeldata = fetch_channeldata()  # Ensure you have defined this function
        if channeldata:
            channel_df = pd.DataFrame(channeldata, columns=[i[0] for i in cursor.description])
            st.dataframe(channel_df)
        else:
            st.warning("No channel data available.")
    elif selected_table == "Video Data":
        st.header("Video Data")
        video_data = fetch_video_data()  # Ensure you have defined this function
        if video_data:
            video_df = pd.DataFrame(video_data, columns=[i[0] for i in cursor.description])
            st.dataframe(video_df)
        else:
            st.warning("No video data available.")
    elif selected_table == "Comment Data":
        st.header("Comment Data")
        comment_data = fetch_comment_data()  # Ensure you have defined this function
        if comment_data:
            comment_df = pd.DataFrame(comment_data, columns=[i[0] for i in cursor.description])
            st.dataframe(comment_df)
        else:
            st.warning("No comment data available.")

    # Close MySQL connection
    mysql_conn.close()

if select_option == "mangodb data":
    # MongoDB connection
    con_obj = pymongo.MongoClient("mongodb://preethi:guvi2024@ac-40kiqlq-shard-00-00.8zhqhxd.mongodb.net:27017,ac-40kiqlq-shard-00-01.8zhqhxd.mongodb.net:27017,ac-40kiqlq-shard-00-02.8zhqhxd.mongodb.net:27017/?ssl=true&replicaSet=atlas-3eyd5j-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster0")
    youtube_db = con_obj["preethika"]
    youtube_edata = youtube_db["empty"]
    youtube_cdata = youtube_db["Channels"]
    youtube_vdata = youtube_db["videos"]
    youtube_codata = youtube_db["comments"]

    # Streamlit UI
    st.subheader("Select MongoDB Collection")

    # Radio button to select collection
    selected_collection = st.radio("Select MongoDB Collection:", ("select the options", "Channels", "videos", "comments"))

    # Fetch and display data based on the selected collection
    if selected_collection == "select the options":
        data = youtube_edata.find()
    elif selected_collection == "Channels":
        data = youtube_cdata.find()
    elif selected_collection == "videos":
        data = youtube_vdata.find()
    else:
        data = youtube_codata.find()

    # Display data in a Streamlit table
    if data:
        st.write("Data from selected MongoDB collection:")
        for document in data:
            st.write(document)
    else:
        st.warning("No data available in the selected MongoDB collection.")

    # Close MongoDB connection
    con_obj.close()

# Define queries
queries = {
    #1
    
    "1.What are the names of all the videos and their corresponding channels?": """
        SELECT v.Video_Name, c.Channel_Name
        FROM videos AS v
        INNER JOIN channeldata AS c ON v.Channel_Id = c.Channel_Id
    """,
      #2  
    "2.Which channels have the most number of videos, and how many videos do they have?": """
        SELECT c.Channel_Name, COUNT(*) AS Video_Count
        FROM videos AS v
        INNER JOIN channeldata AS c ON v.Channel_Id = c.Channel_Id
        GROUP BY c.Channel_Id
        ORDER BY Video_Count DESC
        LIMIT 10
    """,
    #3
    "3.Execute SQL query to find top 10 most viewed videos and their respective channels?":"""
    SELECT v.Video_Name, c.Channel_Name, v.Views
    FROM videos AS v
    INNER JOIN channeldata AS c ON v.Channel_Id = c.Channel_Id
    ORDER BY v.Views DESC
    LIMIT 10
    """,
    #4
    "4.Execute SQL query to find number of comments made on each video and their corresponding video names?": """
    SELECT v.Video_Name, COUNT(*) AS Comment_Count
    FROM videos AS v
    LEFT JOIN video_comments AS vc ON v.Video_Id = vc.Video_Id
    GROUP BY v.Video_Id
    """,
    #5
   "5.Execute SQL query to find videos with the highest number of likes and their corresponding channel names?": """
    SELECT v.Video_Name, c.Channel_Name, v.Likes
    FROM videos AS v
    INNER JOIN channeldata AS c ON v.Channel_Id = c.Channel_Id
    ORDER BY v.Likes DESC
    """,
    #6
    "6.Execute SQL query to find the total number of likes and dislikes for each video?":"""
     SELECT
        v.Video_Name,
        c.Channel_Name,
        v.Likes
    FROM
        videos AS v
    INNER JOIN
        channeldata AS c ON v.Channel_Id = c.Channel_Id
    ORDER BY
        v.Likes DESC;

    """,
    #7
   "7.Execute SQL query to find the total number of views for each channel?": """
    SELECT c.Channel_Name, SUM(v.Views) AS Total_Views
    FROM videos AS v
    INNER JOIN channeldata AS c ON v.Channel_Id = c.Channel_Id
    GROUP BY c.Channel_Id
    """,
    #8
    "8.Execute SQL query to find the names of channels that have published videos in the year 2022?": """
    SELECT DISTINCT c.Channel_Name
    FROM channeldata AS c
    INNER JOIN videos AS v ON c.Channel_Id = v.Channel_Id
    WHERE YEAR(v.Published_Date) = 2022
    """,
    #9
    "9.Execute SQL query to find the average duration of all videos in each channel?": """
   SELECT
    c.Channel_Name,
    IFNULL(AVG(TIME_TO_SEC(v.Duration)), 0) AS Avg_Duration_Seconds
FROM
    videos AS v
INNER JOIN
    channeldata AS c ON v.Channel_Id = c.Channel_Id
GROUP BY
    c.Channel_Id;

   """,
   #10
   "10.Execute SQL query to find videos with the highest number of comments and their corresponding channel names?":"""
    SELECT v.Video_Name, c.Channel_Name, COUNT(vc.Comment_Id) AS Comment_Count
    FROM videos AS v
    INNER JOIN channeldata AS c ON v.Channel_Id = c.Channel_Id
    LEFT JOIN video_comments AS vc ON v.Video_Id = vc.Video_Id
    GROUP BY v.Video_Id
    ORDER BY Comment_Count DESC
   """
   }
#select the query
selected_query = st.selectbox("Select a query", list(queries.keys()))

# Execute and print the results of the selected query
if selected_query:
    cursor = mysql_conn.cursor()
    cursor.execute(queries[selected_query])
    results = cursor.fetchall()
    cursor.close()

    st.write(f"### Results of '{selected_query}':")
    # Display results in a table with corresponding column names
    if results:
        col_names = [desc[0] for desc in cursor.description]
        st.table( [col_names] + results)


mysql_conn.close()
