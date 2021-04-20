
![image](https://user-images.githubusercontent.com/48939255/115102602-8637db00-9f11-11eb-8eb9-717740e9fdba.png)


![image](https://user-images.githubusercontent.com/48939255/115347935-a537a680-a177-11eb-81a4-d38eb5c7069c.png)


![image](https://user-images.githubusercontent.com/48939255/115348036-bd0f2a80-a177-11eb-886a-dfe325445bd4.png)


![image](https://user-images.githubusercontent.com/48939255/115348056-c4363880-a177-11eb-828f-0554e11014fb.png)


![image](https://user-images.githubusercontent.com/48939255/115348108-d44e1800-a177-11eb-8a58-1284b7da8ad0.png)


![image](https://user-images.githubusercontent.com/48939255/115348986-ea100d00-a178-11eb-8141-43d94aa0fdfb.png)


![image](https://user-images.githubusercontent.com/48939255/115415290-65df7900-a1bc-11eb-8928-d8b61838ca32.png)


![image](https://user-images.githubusercontent.com/48939255/115416339-501e8380-a1bd-11eb-998e-46c867168941.png)


![image](https://user-images.githubusercontent.com/48939255/115416376-5a408200-a1bd-11eb-92a0-5c907ab13bf4.png)

![image](https://user-images.githubusercontent.com/48939255/115430537-1011cd80-a1ca-11eb-9ecf-91d31e1673bc.png)




Project:
---
Developing a data pipeline that creates an analytical database  for querying information about the reviews and ratings hosted on redshift database. The main goal of this project is to build an end to end data pipeline which is capable of big volumes of data.

## Scope of the Project
Airbnb wants to analyze the historical data of all the listings on its platform since its initial stages and improve the their recommendations to their customers. To do this, they need to gather average rating, number of ratings and prices of the airbnb listings over the years. As a data engineer of the company, I took up the task of building an ETL pipeline that extracts the releveant data like listings, properties, hosts details and load it to data warehouse that makes querying for the decision makers and analysts easier. 


## Data Description and Sources

The data has been scoped from an awesome site (inside-airbnb)[http://insideairbnb.com/get-the-data.html] which contains the Airbnb actual data. This dataset contains the information about various aspects of Reviews, Calendar, Listings of many cities. As I was interested in Austin and LA, I took the respective data and tried to extract a meaningful information. 
The data comes in three files namely REVIEWS, LISTINGS, CALENDAR.
* Reviews File contains all the reviews of the listing on Airbnb website
  * This file contains more than 1,000,000 rows/records.
* Listing File contains all the house listings on Airbnb
  * The number of listing in Austin and LA come around 40000 rows/records.
* Calendar file contains the availability of the listing across a huge range of dates
  * This file contains more than 13,000,000 rows/records.


## Exploring and Assessing the Data
First step is to explore the data to identify the data quality issues like null values, duplicate data etc.
```Calendar``` and  ```Reviews``` datasets are of good quality, but contains some null column values. If the listing_id and host_id are null, the rows are dropped from the table.
```Listing``` dataset is which contains a lot columns. Making sense from all the columns would take time. In our case we going to use only subset of columns related to the goal of this project. Again this dataset contains many null values. Some columns with null values have been dropped completely. After dropping the null columns, we will be uploading the full dataset into data lake so as to have the ability to use thew other columns that we weren't able to use in this project.

#### Decision Taken regarding Tools, technologies, and data model:
The tools used in this projects are notebooks, Apache Spark, Amazon S3, Amazon Redshift, Apache Airflow.
* To explore the dataset, I started with Google Colab free computing resources with Apache Spark. Spark is better in handling huge data records. Spark provides a great performace as it stores data in-memory shared across the cluster.
* The data lake is stored on Amazon S3, an object storgae service that offers scalability, data availability, security and performance. S3 is perfect for storing data partitioned and grouped in files for low cost and with a lot of flexibility.
* For ETL process, I used an EMR Cluster on AWS.
* To orchestarte the overall data pipeline, I used Apache Airflow as it provides and intutitive UI helping us to track the progress of our pipelines.


## Define the Data Model
The final data model includes 4 dimension tables and 1 Fact table.
This data model will facilitate the daily reporting tasks for the teams reagrding who are the hosts, what are the properties listed, what are the most reviews, reviewers, which neighbourhood and help in building recommendations to the users and improve the services. 

#### Explanation of the Data Model:
For this project we are using building a optimized data lake which can be useful to analytics. The data is prepared, compressed and partitioned by certain columns to allow for fast queries.
We are constructing a snow-flake schema with 1 fact table and multiple dimension Tables.
The 4 Dimension Tables:
* DIM_HOSTS: All the essential information of the hosts with their listing IDs
* DIM_PROPERTIES: All the information about each property and its attributes
* DIM_CALENDARS: Information about the property listing of its availbility, adjusted price, etc
* DIM_REVIEWS: Information about reviews submitted by users for every listing they stayed including the information like reviewer name and date of the review 

The Fact Table ```Airbnb_facts_Austin_LA``` contains the information about the property listing id, corresponding host_id, neighbourhood and important measures like number of reviews, average review ratings and potential earnings. 


## ETL to Model the Data -- The Data Pipeline



Addressing Other Scenarios
* ****The data was increased by 100x.****
  * At present the highest size of the data we have used is less than 1GB. So increasing 100x scenario would not be considered as a major issue, because Amazon's Redshift or S3 are commonly known for reliability and can deal with VERY large data. Thus, in the case of this scenario, I expect that the size of the S3 bucket would be increased and according to that, the tables in Redshift would grow too. Also we could use more nodes EMR cluster for faster processing.
  * But there may be an issue with airflow container. In production, Airflow should be run on a cluster of machines.
* ****The pipelines would be run on a daily basis by 7 am every day.****
  * This scenario can be dealt easily as we are using Apache Airflow. The teams can easily set the Airflow pipelines to a schedule interval to be daily at 7 am on.
* ****The database needed to be accessed by 100+ people.****
  * As mentioned before, Amazon web services are commonly known for its stability and scalability features. So, it would not be considered as an issue or even needed major changes in the platform to be done properly.
