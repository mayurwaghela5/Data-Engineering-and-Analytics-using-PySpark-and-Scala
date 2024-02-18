Server-Side Computing Project Readme
Project Overview

This project was completed as part of the Server-Side Computing course during my Masters degree. The primary objective of the project was to develop applications capable of processing and analyzing large-scale datasets. Specifically, the project involved reading and converting raw text files into more manageable formats such as CSV or Parquet. To achieve this, PySpark's API and Spark SQL were leveraged for efficient data processing, querying, and analytics.
Key Features

- Large-scale Data Processing: The project focused on handling large-scale datasets efficiently, ensuring that data processing tasks could be completed within reasonable timeframes. Datasets of more than 100 GB were routinely used.
- Spark Cluster Utilization: A Spark cluster with 10 working nodes was created and utilized for the labs and the final project in the course. This allowed the entire class to run their queries and gain analytics in parallel, significantly speeding up the processing time.
- Data Conversion: Raw text files were converted into CSV or Parquet formats, making the data more accessible for analysis and manipulation.
- PySpark Integration: PySpark's powerful API was utilized for distributed data processing, enabling parallel execution across multiple nodes for improved performance.
- Spark SQL Queries: Spark SQL was employed for querying and analyzing the processed data, facilitating various analytical tasks.
- Performance Optimization: Measures were taken to optimize performance, resulting in enhanced scalability. Overall, performance was improved by 30% compared to baseline implementations.
- Storage in MinIO S3: The processed data was stored in MinIO S3, ensuring durability, scalability, and accessibility for future use.