## Data Warehouse Project
---
Sparkify, a music streaming app has grown its user base and song database and want to move their processes and data onto the cloud. For now thier data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, we are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

The data sources to ingest into data warehouse are provided by two public S3 buckets:
* Songs bucket (s3://udacity-dend/song_data), contains info about songs and artists. All files are in the same directory.
* Event bucket (s3://udacity-dend/log_data), contains info about actions done by users, what song are listening, ... 

We have differents directories so we need a descriptor file (also a JSON) in order to extract data from the folders by path. We used a descriptor file (s3://udacity-dend/log_json_path.json) because we don't have a common prefix on folders

---

Approach:
---
#### Creating Database
1. Create a user on aws, give programatical, administrator permission and note down the Access and Secret KEY.
2. Fill dwh.cfg file with key values and start running ```clear_cluster``` notebook to create a new redshift cluster and gather aditional details of the cluster infrastructure required for cfg file.
    * Launch Redshift cluster and create IAM role that has access to S3
    * Add redshift database and IAM role info to dwh.cfg.
3. Following the notebook, open an incoming TCP port to access the cluster host/endpoint thereby connecting to the cluster.
4. Develop fact and dimesion tables of the star schema in Redshift using the ```create_table.py``` &  ```sql_queries.py``` file.
    * Drop statements, create statements commands are written in these files
    * This file contains how the data is gathered from amazon s3 buckets
    * ```create_table.py``` -- This script will drop old tables (if exist) ad re-create new tables/
        * Run ```create_table.py``` every time we resetting of the database and test the ETL pipeline is needed.
    * Test by running ```create_table.py``` and checking the schemas in redshift database.
    * ```sql_queries.py``` contains all transformatios in SQL to be done.
    

#### Building ETL Pipeline
1. Logic in ```etl.py``` scripts ingests data from S3 to stagging tables in Redshift
2. Commands in ```etl.py``` writes/inserts data from stagging tables to dimesion and analytical tables on Redshift from S3 
3. Test by running ```etl.py``` after running ```create_table.py```
4. We can run any type of analytical queries to compare our results


#### Delete the Redshift cluster
* Go back to notebook and run the delete cluster using the command code


Database Schema:
---

* Staging Tables
    * staging_events
    * staging_songs
* Fact Table
    * songplays - records in event data associated with song plays i.e. records with page NextSong 
    * - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
* Dimension Tables
    * users - users in the app - user_id, first_name, last_name, gender, level
    * songs - songs in music database - song_id, title, artist_id, year, duration
    * artists - artists in music database - artist_id, name, location, lattitude, longitude
    * time - timestamps of records in songplays broken down into specific units - start_time, hour, day, week, month, year, weekday
