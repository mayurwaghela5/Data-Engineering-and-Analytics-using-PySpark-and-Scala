# Part One

  - Steps to excute Part One
  
      - Login into spark server using credentials: `ssh -i /Users/mayurwaghela/Documents/itmd-521/jammy46/id_ed25519_spark_edge_key mwaghela@system45.rice.iit.edu`
      - Navigate to the folder `~/mwaghela/itmd-521/final/part-one`
      - For the first section run below command for converting the file `30.txt` to cvs, json, csv with lz4 compression, parquet, csv with a single partition
      
        `nohup spark-submit --master spark://sm.service.consul:7077 --packages "org.apache.hadoop:hadoop-aws:3.2.3,com.mysql:mysql-connector-j:8.0.32" --executor-memory 6g --executor-cores 2 --num-executors 12 --total-executor-cores 24 --driver-memory 6g --proxy-user controller MW-minio-read-and-process-30.py &`

      - For the Second section run the below three commands for converting the file to cvs, json, and parquet files

      - Command for 40.txt: 
      
        `nohup spark-submit --master spark://sm.service.consul:7077 --packages "org.apache.hadoop:hadoop-aws:3.2.3,com.mysql:mysql-connector-j:8.0.32" --executor-memory 6g --executor-cores 2 --num-executors 12 --total-executor-cores 24 --driver-memory 6g --proxy-user controller MW-minio-read-40.py &`

      - Command for 60.txt: 
      
        `nohup spark-submit --master spark://sm.service.consul:7077 --packages "org.apache.hadoop:hadoop-aws:3.2.3,com.mysql:mysql-connector-j:8.0.32" --executor-memory 6g --executor-cores 2 --num-executors 12 --total-executor-cores 24 --driver-memory 6g --proxy-user controller MW-minio-read-60.py &`

      - Command for 80.txt: 
      
        `nohup spark-submit --master spark://sm.service.consul:7077 --packages "org.apache.hadoop:hadoop-aws:3.2.3,com.mysql:mysql-connector-j:8.0.32" --executor-memory 6g --executor-cores 2 --num-executors 12 --total-executor-cores 24 --driver-memory 6g --proxy-user controller MW-minio-read-80.py &`


# Part Two

- Steps to execute part two
    - Run the below command to read a csv from our own bucket and convert and write into a json, parquet and into MariaDB
    
        `nohup spark-submit --master spark://sm.service.consul:7077 --packages "org.apache.hadoop:hadoop-aws:3.2.3,com.mysql:mysql-connector-j:8.0.32" --executor-memory 6g --executor-cores 2 --num-executors 12 --total-executor-cores 24 --driver-memory 6g --proxy-user controller MW-minio-read.py > ./nohup-read-final1.out &`


# Part three

  - Steps to execute part three
    - We will execute the commands with different parameters and take note of the execution time

        Command 1: 
        
        `nohup spark-submit --master spark://sm.service.consul:7077 --packages "org.apache.hadoop:hadoop-aws:3.2.3,com.mysql:mysql-connector-j:8.0.32" --executor-memory 4g --executor-cores 1 --num-executors 20 --total-executor-cores 20 --driver-memory 2g --proxy-user controller minio-50-parquet-one.py > ./nohup-run-one.out &20 --total-executor-cores 20 --driver-memory 2g --proxy-user controller minio-50-parquet-one.py > ./nohup-run-one.out &`

        Command 2: 
        
        `nohup spark-submit --master spark://sm.service.consul:7077 --packages "org.apache.hadoop:hadoop-aws:3.2.3,com.mysql:mysql-connector-j:8.0.32" --executor-memory 12g --executor-cores 2 --driver-memory 10g --proxy-user controller minio-50-parquet-two.py > ./nohup-run-two.out &`

        Command 3: 
        
        `nohup spark-submit --master spark://sm.service.consul:7077 --packages "org.apache.hadoop:hadoop-aws:3.2.3,com.mysql:mysql-connector-j:8.0.32" --executor-memory 4g --executor-cores 2 --num-executors 20 --total-executor-cores 40 --driver-memory 4g --proxy-user controller minio-50-parquet-three.py > ./nohup-run-three.out &`


# Part 4

  - Steps to execute part four
    - Run the below command to perform some analytics and write the results to parquet files
        
        `nohup spark-submit --master spark://sm.service.consul:7077 --packages "org.apache.hadoop:hadoop-aws:3.2.3,com.mysql:mysql-connector-j:8.0.32" --executor-memory 4g --executor-cores 1 --num-executors 20 --total-executor-cores 20 --driver-memory 2g --proxy-user controller part-four-1.py > ./nohup-part-four-1.out &`
    