# Data-Engineering-Concepts

### Data Engineering skills and tools
* Languages: Python, SQL
*  Databases: Postgres, Mysql, AWS RDS, DynamoDB, RedShift, Apache Cassandra
* Modeling: Dimensional data modeling
* Batch ETL: Python, SparkSQL
* Workflow Management: Airflow

### Data Engineering projects/Concepts Learnt
1. #### [Data Modeling](https://github.com/srujanreddyj/data-engineering-concepts/tree/master/Postgres-cassandra)
   * Created a relational database using PostgreSQL to fit the diverse needs of data consumers.
   * Developed a Star Schema database using optimized definitions of Fact and Dimension tables. Normalization of tables.
   * Created a nosql database using Apache Cassandra 
   * Developed denormalized tables optimized for both specific set queries and business queries
   * An ETL pipeline was build databases in PostgreSQL and Apache Cassandra.
   * ***Proficiencies learned and used: Python, PostgreSql, Star Schema, ETL pipelines, Normalization, Apache Cassandra, Denormalization***
2. #### [Data Warehouses](https://github.com/srujanreddyj/data-engineering-concepts/tree/master/datawarehouse)
   * Applied Data Warehouse architectures learnt and built a data warehouse on AWS cloud. 
   * Developed an ETL pipeline that extracts data from **S3 buckets**, stages them in **Redshift cluster**, and transforms the data into **dimension and fact tables** for analytics teams.
   * Learnt more on Amazon Redshift CLusters, IAM Roles, Security Groups
   * ***Proficiencies learned and used: Python, Amazon Redshift, aws cli, Amazon SDK, SQL, PostgreSQL***
3. #### [Data Lakes](https://github.com/srujanreddyj/data-engineering-concepts/tree/master/Datalake)
   * Built a data lake on **AWS Cloud using Spark and AWS EMR CLuster**. 
   * Scaled up the current ETL pipeline by moving the data warehouse to a data lake.
   * The Data lake acts as a single source analytics palform and an ETL jobs are written in Spark that extracts data from **S3**, stages them in **Redshift**, process the data into **analytics tables** using **Spark**, and loads them back into **S3**.
   * ***Proficiencies learned and used: Spark, S3, EMR, Athena, Parquet.***
4. #### [Data Pipelines with Airflow](https://github.com/srujanreddyj/data-engineering-concepts/tree/master/airflow)
   * Created and automated a set of data pipelines with Airflow and Python.
   * Wrote custom operators, plugins to perform tasks like staging data, tranforming data into star schema by creating dimension and fact tables, and validation through data quality checks
   * I scheduled ETL jobs in Airflow, created project related plugins, operators and automated the pipeline execution leading to better monitoring and debugging production pipelines. 
   * ***Proficiencies learned and used: Apache Airflow, S3, Amazon Redshift, Python.***

