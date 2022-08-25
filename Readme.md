# DSP course


## Task 1 : Manager-Worker Map-Reduce Infrastructure  

- ami used: ```"ami-0cff7528ff583bf9a  //  standard amazon linux ami``` , type used: ```t2.MICRO```

## Implementation
### usage
```
java -jar LocalApp.jar <N> <terminate> <timeout>
```
where:
- N: is number of jobs per worker for the task
- terminate: put 'terminate' string to terminate the program,  
   otherwise use any other none-empty string.
- timeout: specify the time interval (in minutes) as an integer.  
  jobs in this task who takes longer than the specified amount will be discarded.

### Overall Design Rundown

Local App starts running and starts a manager instance if one is not found. It sends the manager its request through an SQS.

There are two queues connecting the local app to the manager - one for outgoing requests (Tasks) and one for incoming responses.
 The local app generates a unique key for the task and uploads the input file to an S3 bucket dedicated for outgoing requests.  

The manager listens on the Task queue and when a new message arrives it extracts the file location and downloads it.
 The manager breaks down the tasks to Jobs, where each job represents a file in the task to be parsed. 

Likewise, the manager uses 2 queues to communicate with the workers - one for incoming, completed jobs and one for outgoing, pending ones.

The workers download the file, parse it, upload the parse result to another bucket (dedicated for jobs' outputs) and send the file-url on S3 to the manager.
 
 The manager keeps track of the status of the jobs comprising each task.
  When a task is ready, the manager writes all of the jobs' outputs into a single file, uploads it to a third bucket, and send the output-file's location back to the local app.

### Manager
The manager's work is preformed using 3 threads. The first one listens on the Incoming tasks queue, preforming long polls for resource efficiency.
When it receives a new message it leaves a Runnable in a blocking queue for the main thread to perform. The runnable processes the message and creates the new jobs for the workers to do.
If a terminate message is received, a different runnable would change the manager state in order to announce termination.

A second thread listens on the completed job queue. similarly it leaves runnables in a blocking queue which handle the incoming messages.
The runnables update the states of the relevant job, and check whether the whole task is complete - it checks the status of each job in the task,
and checks to see if there is a job who has timed out (timeout value passed as a parameter from the local app's arguments to the manager).
If the task is done, the runnable sums up the jobs to a single file , uploads it, and sends it's location to the local app.

The third thread takes runnables from the blocking queue (errandQueue) one by one, and runs them.        



### Contingencies
- If a worker node dies it's job will eventually timeout.The next time the manager polls the incoming task queue, it will
    re-evaluate the necessary-workers amount and create new worker instances.
- When a termination message arrives in the incoming tasks queue, the manager updates is state (a private volatile field).
    When all former tasks are done it terminates the workers, purges the queues and closes itself.
- A maximum-workers amount was hard-coded to ensure the system doesn't exceed the maximum instances limit.
- The manager doesn't do any of the parsing work. Each worker node's work is completely agnostic to the any other worker node's work.      

## Rsults