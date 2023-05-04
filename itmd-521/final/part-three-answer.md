 # Part Three- Execution and Answers


# First run
* `--driver-memory 2G --executor-memory 4G --executor-cores 1 --total-executor-cores 20`

    The job was in intially in queue and the real execution time is being calculated with the help of result .out file.
    
    * Started: 23/05/01    13:13:05
    * Ended: 23/05/01      14:09:23

    Execution time: `56 mins 18 secs`


* Your Expectation: Since it takes only one worker node i.e 1 executor core, the execution time will be long. The driver-memory allocated is enough as the actions and transformation on the RDD is not too much. But the executor memory allocated is higher than the driver memory which is not a good case. So ideally not good enough and the execution would be slow
* Your results/runtime: It took around 56 mins for the whole job. 

![3_1file](./resultimages/Part3-1.png "3_1file") 


# Second run
* `--driver-memory 10G --executor-memory 12G --executor-cores 2`

    The job was in intially in queue and the real execution time is being calculated with the help of result .out file.
    
    * Started: 23/05/01    14:29:52
    * Ended: 23/05/01      15:18:57

    Execution time: `49 mins 5 secs`
* Your Expectation: The driver memory allocated for this job is too much. The executor memory which is the memory to execute a single task in the job is also very high. There would be memory hogging. But since the number of cores is 2 and the total executor cores used were 64 in this case, the execution time would be better than the first case. 
* Your results/runtime: It took around 49 mins which is better than the first case as expected.

![3_2file](./resultimages/Part3-2.png "3_2file") 



# Third run
* `--driver-memory 4G --executor-memory 4G --executor-cores 2 --total-executor-cores 40`

    The job was in intially in queue and the real execution time is being calculated with the help of result .out file.
    
    * Started: 23/05/01    14:09:23
    * Ended: 23/05/01      14:56:50

    Execution time: `47 mins 27 secs`

* Your Expectation: In this case the driver memory and the executor memory are allocated in a way to avoid the job to run slow due to excessive memory and less resources used. Also the number of cores used is 2 and total executor cores used are 40. So ideally the execution time should be the fastest as compared to other two scenarios.
* Your results/runtime: It took around 47 mins to complete this execution which is better than the other two cases.

![3_3file](./resultimages/Part3-3.png "3_3file") 
