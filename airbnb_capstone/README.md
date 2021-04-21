# DATA ENGINEERING CAPSTONE PROJECT

## Project
Developing a data pipeline that creates an analytical database for querying information about the reviews and ratings hosted on the redshift database. The main goal of this project is to build an end-to-end data pipeline that is capable of big volumes of data.

## Scope of the Project
Airbnb wants to analyze the historical data of all the listings on its platform since its initial stages and improve its recommendations to its customers. To do this, they need to gather the average rating, number of ratings, and prices of the Airbnb listings over the years. As a data engineer of the company, I took up the task of building an ETL pipeline that extracts the relevant data like listings, properties, hosts details, and load it to a data warehouse that makes querying for the decision-makers and analysts easier.  

#### End-Use Cases
* Query-based analytical tables that can be used by decision-makers
* Analytical Table that can be used by analysts to explore more and develop recommendations for users.

## Data Description and Sources

The data has been scoped from an awesome site [inside-airbnb](http://insideairbnb.com/get-the-data.html) which contains the Airbnb actual data. This dataset contains information about various aspects of Reviews, Calendars, Listings of many cities. As I was interested in Austin, TX and Los Angeles (LA), CA, I took the respective data and tried to extract meaningful information. 
The data comes in three files namely ```REVIEWS, LISTINGS, CALENDAR```.
* ```Reviews``` File contains all the reviews of the listing on the Airbnb website
  * This file contains more than ***1,000,000 rows/records***.
* ```Listing``` File contains all the house listings on Airbnb
  * The number of listing in Austin and LA come around ***40000 rows/records***.
* ```Calendar``` file contains the availability of the listing across a huge range of dates
  * This file contains more than ***13,000,000 rows/records***.


## Tools and Technologies
The tools used in this project are notebooks, Apache Spark, Amazon S3, Amazon Redshift, Apache Airflow.
* To explore the dataset, I started with Google Colab free computing resources and Apache Spark. 
  * Spark is better at handling huge data records. Spark provides a great performance as it stores data in-memory shared across the cluster.
* The data lake is stored on Amazon S3, an object storage service that offers scalability, data availability, security, and performance. 
  * S3 is perfect for storing data partitioned and grouped in files for low cost and with a lot of flexibility.
  * Also it has the flexibility in adding and removing additional data
  * Ease of schema design and availability to a wide range of users
* For the ETL process, I used an EMR Cluster on AWS Redshift.
* To orchestrate the overall data pipeline, I used Apache Airflow as it provides an intuitive UI helping us to track the progress of our pipelines.
## Exploring and Assessing the Data
The first step is to explore the data to identify the data quality issues like null values, duplicate data, etc.
 * ```Calendar``` and  ```Reviews``` datasets are of good quality but contain some null column values. 
   * If the listing_id and host_id have null column values, then the respective rows are dropped from the table.
* ```Listing``` dataset is which contains a lot of columns. 
  * Making sense from all the columns would take time. In our case, we are going to use a subset of columns related to the goal of this project. 
  * Again this dataset contains many null values. Columns values with null in listing_id, host_id have been dropped. 
  * After dropping the null columns & rows, we will be uploading the full dataset into the data lake to have the ability to use the other columns that we weren't able to use in this project.


## Defining the Data Model
The final data model includes 4 dimension tables and 1 Fact table.
This data model will facilitate the daily reporting tasks for the teams regarding who are the hosts, what are the properties listed, what are the most reviews, reviewers, which neighborhood and help in building recommendations to the users and improve the services. 

#### Explanation of the Data Model
For this project, we are building an optimized data lake that can be useful to analytics. The data is prepared, compressed, and partitioned by certain columns to allow for fast queries.
We are constructing a snow-flake schema with 1 fact table and multiple dimension tables.
After reviewing and cleaning the datasets, we are required to build three staging table which can be used for preprocessing the data before loading into the Data warehouse tables. 

***DATA DICTIONARY***

The Data Warehouse tables are the Fact and Dimension Tables:
* ***Dimension Tables***:
  * ``` DIM_HOSTS```: All the essential information of the hosts with their listing IDs.
  * ``` DIM_PROPERTIES```: All the information about ***each property and its attributes***.
  * ```DIM_CALENDARS```: Information about the property listing of its availability, adjusted price, etc.
  * ``` DIM_REVIEWS```: Information about reviews submitted by users for every listing they stayed including the information like reviewer name and date of the review.
* ***The Fact Table***
  * ```FACT_Airbnb_Austin_LA``` contains the important measures like ***number of reviews, average review ratings, and potential earnings*** along with the information about the corresponding property listing id, host_id, neighborhood and. 


![Schema-based Data Model](https://user-images.githubusercontent.com/48939255/115430537-1011cd80-a1ca-11eb-9ecf-91d31e1673bc.png)

## ETL to Model the Data -- The Data Pipeline
The design of the pipeline can be summarized as:
* Extract data from source S3 location.
* Process and Transform it using Apache Spark, SQL, Python
* Load a cleaned dataset and intermediate artifacts to S3 destination locations.
* Build dimension tables and fact table by calculating summary statistics/measures using EMR Cluster, SQL and Airflow operators.

The idea behind using the datalakes is that they provide us with the flexibility in the number of different ways we might use the data.

* Data Processing
  *  For Data Processing, I used SQL to process data from S3 bucket. For each task, an SQL statement has been providded in ```SQLQUEIRES.py``` which does the data ingestion process smoothly.
  *  This data processing file contains all the queries to create tables, inserting data from stagging tables and building query tables.

* ETL pipeline includes 20 tasks:
  * ```START_OPERATOR```, ```MID_OPERATOR```, ```END_TASK``` are the dummy tasks, which help in starting and ensuring all tasks are syncronized with each other tasks and finished the execution
  * ```CREATE_STAGGING_REVIEWS_Table```, ```CREATE_STAGGING_CALENDARS_Table```, ```CREATE_STAGGING_LISTINGS_Table``` are the tasks for creating Stagging tables on Redshift Cluster.
    *  These tasks are created using ```CreateTablesOperator``` which includes a PostgresOperator
  * ```STAGE_REVIEWS```, ```STAGE_CALENDARS```, ```STAGE_LISTINGS``` are the tasks responsible for loading the data from S3 to Redshift cluster.
    * These tasks are created using the ```StagetoRedshiftOperator``` 
  * ```CREATE_Table_DIM_HOSTS```, ```CREATE_Table_DIM_PROPERTIES```, ```CREATE_Table_DIM_CALENDARS```, ```CREATE_Table_DIM_REVIEWS```, ```CREATE_Table_FACT_AIRBNB``` are the tasks for creating Dimensions tables and fact table on Redshift cluster.
     *  These tasks are created using ```CreateTablesOperator``` which includes a PostgresOperator
  * ```LOAD_TABLE_DIM_PROPERTIES```, ```LOAD_TABLE_DIM_HOSTS```, ```LOAD_TABLE_DIM_REVIEWS```, ```LOAD_TABLE_DIM_CALENDARS``` are the tasks for copying the data from Stagging Tables with respective conditions.
     * These tasks are created using the ```LoadDimensionOperator```
  * ```LOAD_Fact_AIRBNB_AUSTIN_LA_TABLE``` is the task for measuring events from dimensions tables to build a query-based fact table for decision makers.
     * These tasks are created using the ```LoadFactOperator```
  * ```RUN_DATA_QUALITY_CHECKS``` is the task for performing data quality checks by running sql statements to validate the data and ensures that the specified table has rows
     * These tasks are created using the ```DataQualityOperator```



***AIRFLOW OPERATORS in the Pipeline:***
![image](https://user-images.githubusercontent.com/48939255/115416339-501e8380-a1bd-11eb-998e-46c867168941.png)

***COMPLETE DATA PIPELINE DESIGN in AIRFLOW***

This DAG is responsible for the ETL Process and creating a datalake.

![image](https://user-images.githubusercontent.com/48939255/115415290-65df7900-a1bc-11eb-8928-d8b61838ca32.png)

***TREE GRAPH of ETL Pipeline:***

![image](https://user-images.githubusercontent.com/48939255/115348036-bd0f2a80-a177-11eb-886a-dfe325445bd4.png)


***KANBAN of ETL pipeline to complete:***

![image](https://user-images.githubusercontent.com/48939255/115416376-5a408200-a1bd-11eb-92a0-5c907ab13bf4.png)

## Running the Project
1. Explore the dataset as mentioned in above notebook file, transform the data and store the processed result in S3.
2. Create AWS Redshift Cluster using either the console or through the CLI.
3. Ensure the airflow instance is up and running.
4. Ensure all the content of dags and plugins are present in the Airflow work environment as needed.
5. Create AWS Connection and Postgres Redshift Connection Ids by providing AWS Access KEY ID, Secret Access Key, Cluster Name URL, Database Name, User and Password, Post Number
6. Actiavte DAG and run it.



## Addressing Other Scenarios
* ****The data was increased by 100x.****
  * The pipeline can be made autoscalling enabled and this will help bigger amount of data be processed without many bottlenecks. 
  * At present the highest size of the data we have used is less than 1GB. So increasing 100x scenario would not be considered as a major issue, because Amazon's Redshift or S3 are commonly known for reliability and can deal with VERY large data. Thus, in the case of this scenario, the size of the S3 bucket would be increased and accordingly the tables in Redshift would grow too. 
  * Also we could use increase EMR cluster size to handle larger volumes of data nodes for faster processing.
  * But there may be an issue with airflow container. In production, Airflow should be run on a cluster of machines.
* ****The pipelines would be run on a daily basis by 7 am every day.****
  * This scenario can be dealt easily as we are using Apache Airflow. The teams can easily set the Airflow pipelines to a schedule interval to be daily at 7 am on.
  * Regualar email updates on failures and quality can also be enabled.
* ****The database needed to be accessed by 100+ people.****
  * Amazon web services are known for its stability and scalability features. So, a concurrency limit for the Redshift cluster can be set and also be expanded as deemed necessary.
  * Also AWS comes with auto-scaling capabilities and good read performance and hence would not be considered as an issue and needed major changes in the platform can be done properly.



## LESSONS LEARNED
1. Using Parquet data files is much faster and AWS ability to read these files is superior compared csv when text column values are present. csv files are larger in space compared to parquet files.
2. Null Fields will not be present in parquet and thereby mismatch of columns arise in AWS Redshift.
3. Redshift COPY Command: COPY, INSERT commands in redshift work seemless when the data type of the columns match or else understanding the error message is even more painful
4. Install AIRFLOW on local machines is not as easy as it sounds.
5. Apache Spark SQL and Pyspark solve purpose when there are more **1 million rows**, else better to stick with Python only.
6. AWS is very powerful and has unlimited potential anyone can tap. More importantly need to be careful with the pricing.

---
***References & Acknowledgements:***
Udacity and many github profiles.

