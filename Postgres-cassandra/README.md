Data Modeling with Postgres
---
This project aims to build a database and the ETL process for a music streaming company SPARKIFY. This company has a collection of data on user activity from their music streaming application in JSON files. However, this way of storing data has made things difficult for them during querying and extracting insights from the data.

To solve this difficulty, I am building a Postgres database with relevant tables which will help the Sparkify team to access data easier and generate insights from the users data 

Database Design
----
To build this database, I select a star scheme, as it simplifies generating queries and perform fast aggregations. The fact and dimension tables are built as follows:

![Fact Table - Dimension Table](Slide1.jfif)

The fact table and dimensions table are described as above.

Fact Table:
    *  songplays: The songplay_id field is the primary key and it is an auto-incremental value.
    
Dimensions Tables:
    * users - users in the app : user_id is the primary key 
    * songs - songs in music database: song_id is the primary key
    * artists - artists in music database: artist_id is the primary key
    * time - timestamps of records in songplays broken down into specific units: start_time is the primary key
    
------------------------
Building the database and the ETL Pipeline:
---
1. Start with writing all sql queries for creating and droping tables (if already exists) in `sql_queries.py`
2. Create database and tables mentioned in the `sql_queries.py` using `create_table.py`
3. Starting the ETL Process and Pipeline:
    1. Download all important and necessary libraries like ```psycopg2, pandas, os, glob``` 
    2. Exploring the data in json format in a notebook and developing a process
    3. Using the code from notebook, develop a python script (`etl.py`) to process the entire datasets.
    4. run test.ipynb to test and confirm the records were succesfully inserted into each table.
    
------------------------
Sample Code:
1. Step 1: dropping table if exists -- ``` user_table_drop = "DROP TABLE IF EXISTS users" ```
2. Creating a table with columns -- 
    ~~~ python
    user_table_create = ("""CREATE TABLE IF NOT EXISTS users(user_id INT PRIMARY KEY, 
                                                            first_name varchar, 
                                                            last_name varchar, 
                                                            gender char(1), level varchar)""")
    ~~~
3. Inserting values into the table 
~~~ python
user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
          VALUES(%s, %s, %s, %s, %s)
          ON CONFLICT(user_id) DO UPDATE
                     SET level = EXCLUDED.level;
""")
~~~ 
            
4. Developing a list of actions for all the tables creating and droping
5. Inserting the dataset records into tables
```python
# insert songplay records
for index, row in df.iterrows():

    # get songid and artistid from song and artist tables
    cur.execute(song_select, (row.song, row.artist, row.length))
    results = cur.fetchone()

    if results:
        songid, artistid = results
    else:
        songid, artistid = None, None

    # insert songplay record
    songplay_data = (index, pd.to_datetime(row.ts, unit="ms"), row.userId, row.level, songid, artistid, row.itemInSession, row.location, row.userAgent)
    #songplay_data = 
    cur.execute(songplay_table_insert, songplay_data)      
```


----
## Modelling NoSQL Database or Apache Cassandra Database:
In this project, I would be applying Data Modeling with Apache Cassandra and complete an ETL pipeline using Python. I am provided with a part of the ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.

#### Modelling your NoSQL Database or Apache Cassandra Database:
1. Design tables to answer the queries outlined
2. Write Apache Cassandra CREATE KEYSPACE and SET KEYSPACE statements
3. Develop CREATE statement for each of the tables to address each question
4. Load the data with INSERT statement for each of the tables
5. Include IF NOT EXISTS clauses in your CREATE statements to create tables only if the tables do not already exist. Include DROP TABLE statement for each table, this way we can run drop and create tables whenever you want to reset your database and test your ETL pipeline
6. Test by running the proper SELECT statements with the correct WHERE clause

#### Build ETL Pipeline:
1. Implement the logic in the notebook template to iterate through each event file in event_data to process and create a new CSV file in Python
2. Make necessary edits to Part II of the notebook template to include Apache Cassandra CREATE and INSERT three statements to load processed records into relevant tables in your data model
3. Test by running SELECT statements after running the queries on your database
4. Drop the tables and shutdown the cluster


#### Credits
---
This project was built from a Udacity Nanodegree template.
