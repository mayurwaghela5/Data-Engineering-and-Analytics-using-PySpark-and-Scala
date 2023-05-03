 # Part Three- Execution and Answers

![execution](image/execution.png "execution")

# First run
* `--driver-memory 2G --executor-memory 4G --executor-cores 1 --total-executor-cores 20`
* Your Expectation: It took more time than second and third run, it took 59 mins after giving driver memory 2G and executor memory 4G because allocating resources which are lesser can caused memory hogging so it took more time than another jobs

![3_1file](image/3_1file.png "3_1file")
![result3_1](image/result3_1.png "result3_1")


# Second run
* `--driver-memory 10G --executor-memory 12G --executor-cores 2`
* Your Expectation: It took 57 mins to run after giving driver memory 10G and executor memory 12G because providing more memory for small job cause took more time 
# app-20220428194603-0575	Np part-three executions	10	4.0 GiB		    2022/04/28 19:46:03	npagare	FINISHED	57 min
![result3_2](image/result3_2.png "result3_2")
![resultfollowed3_2](image/resultfollowed3_2.png "resultfollowed3_2")


# Third run
* Your Expectation: Third run took lesser time than first two runs and took 43 mins because the in a cluster where we have other applications running and they also need cores to run the tasks, we need to make sure that we assign the cores at the cluster level at the same time the more memory is not required to run the job. 
# app-20220428194529-0574	Np part-three executions	2	4.0 GiB		    2022/04/28 19:45:29	npagare	FINISHED	43 min

* `--driver-memory 4G --executor-memory 4G --executor-cores 2 --total-executor-cores 40`
![result3.3](image/result3.3.png "result3.3")
![result3.3followed](image/result3.3followed.png "result3.3followed")
