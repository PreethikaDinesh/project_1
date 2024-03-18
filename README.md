# project_1

Data Storage: Fetched data is stored in two types of databases:

SQL Database: Video metadata and related information are stored in a relational database for structured querying.
MongoDB: Comments and other unstructured data are stored in a NoSQL database for flexibility.
Data Warehousing: The stored data is organized and optimized for querying and analysis.

User Interface: Streamlit is used to create a user-friendly interface for querying and visualizing the YouTube data.

Features:
Data Harvesting: Retrieve video metadata, comments, and statistics using the YouTube Data API.
Storage: Store data in SQL and MongoDB databases.
Data Warehousing: Organize and optimize data for efficient querying.
User Interface: Interact with the data through a Streamlit dashboard.
Querying: Perform structured and ad-hoc queries on stored data.
Visualization: Visualize key metrics and trends using charts and graphs.
Requirements
Python 3.x
YouTube Data API key
MySQL or another SQL database server
MongoDB server
Streamlit
Setup:
Install dependencies: pip install -r requirements.txt
Set up API keys for YouTube Data API.
Configure database connections in config.py.
Run the data harvesting scripts to fetch YouTube data.
Populate the SQL and MongoDB databases with the fetched data.
Start the Streamlit app: streamlit run app.py.
Usage:
Open the Streamlit app in your web browser.
Explore available queries and visualizations.
Use the user interface to interact with the YouTube data.
