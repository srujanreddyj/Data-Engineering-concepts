# Data-Engineering-Concepts

### Data Engineering skills and tools
* Languages: Python, SQL
*  Databases: Postgres, Mysql, AWS RDS, DynamoDB, RedShift, Apache Cassandra
* Modeling: Dimensional data modeling
* Batch ETL: Python, SparkSQL
* Workflow Management: Airflow

### Data Engineering projects/Concepts Learnt
1. [Data Modeling](https://github.com/srujanreddyj/data-engineering-concepts/tree/master/Postgres-cassandra)
   * Created a relational database using PostgreSQL to fit the diverse needs of data consumers.
   * Developed a Star Schema database using optimized definitions of Fact and Dimension tables. Normalization of tables.
   * Created a nosql database using Apache Cassandra 
   * Developed denormalized tables optimized for a specific set queries and business needs
   * Used ETL to build databases in PostgreSQL and Apache Cassandra.
   * ***Proficiencies include: Python, PostgreSql, Star Schema, ETL pipelines, Normalization, Apache Cassandra, Denormalization***
2. [Data Warehouses](https://github.com/srujanreddyj/data-engineering-concepts/tree/master/datawarehouse)
   * Applied Data Warehouse architectures learnt and built a data warehouse on AWS cloud. 
   * ETL pipeline that extracts data from **S3**, stages them in **Redshift**, transforms the data into **dimension and fact tables** for analytics teams.
3. [Data Lakes](https://github.com/srujanreddyj/data-engineering-concepts/tree/master/Datalake)
   * Built a data lake on **AWS Cloud using Spark and AWS EMR CLuster**. 
   * Scaled up the current ETL pipeline by moving the data warehouse to a data lake.
   * The Data lake acts as a single source analytics palform and an ETL jobs are written in Spark that extracts data from **S3**, stages them in **Redshift**, process the data into **analytics tables** using **Spark**, and loads them back into **S3**.
4. [Data Pipelines with Airflow](https://github.com/srujanreddyj/data-engineering-concepts/tree/master/airflow)
   * Created and automated a set of data pipelines with Airflow, monitoring and debugging production pipelines. I scheduled ETL jobs in Airflow, created project related  plugins, operators and automated the pipeline execution.

