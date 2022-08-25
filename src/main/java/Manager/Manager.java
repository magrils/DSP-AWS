package Manager;

import Common.*;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.io.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class Manager {
    // Utility-Objects
    private SQSHandler sqs;
    private S3Handler s3;
    private EC2Handler ec2;
    private Logger logger;

    // Manager logic
    private int n; //@TODO: read N from task-message
    private int timeout = 15; //in minutes //@TODO: read time from task-message and store in Task object
    private Set<String> activeWorkerIDs;
    private Map<String, Task> activeTasks;
    private BlockingQueue<Runnable> errandsQ = new LinkedBlockingDeque<>();
    private volatile boolean terminate = false;

    public Manager(SQSHandler sqs, S3Handler s3, EC2Handler ec2, int n) {
        this.sqs = sqs;
        this.s3 = s3;
        this.ec2 = ec2;
        this.n = n;
        this.activeWorkerIDs = new HashSet<>();
        this.activeTasks = new HashMap<>();
        this.logger = new Logger();
    }


    public void mainManagerWork() {
        System.out.println("Starting manager");
        logger.log("Starting manager");
        Thread taskListenerThread
                = new Thread(new Runnable() {
            @Override
            public void run() {
                taskListener();
            }
        });
        System.out.println("Starting task listener");
        logger.log("Starting task listener");
        taskListenerThread.start();

        Thread jobListenerThread
                = new Thread(new Runnable() {
            @Override
            public void run() {
                jobListener();
            }
        });
        System.out.println("Starting job listener");
        logger.log("Starting job listener");
        jobListenerThread.start();

        System.out.println("Starting main manager thread");
        logger.log("Starting main manager thread");
        busyWork();
    }

    public List<Message> waitForMessages(Collection<String> msgsAttToGet, Collection<String> allMsgAtts, String url, int visTime) {
        List<Message> msgsToTake = new ArrayList<>();
        while (true) {
            List<Message> messages = sqs.receiveMessages(url, allMsgAtts, visTime, Params.Max_WaitTime, Params.Max_Messages);
            if (messages != null) {
                int m = 0;
                for (Message message : messages) {
                    //test print
                    m++;
                    System.out.printf("\tchecking message # %d\n", m);
                    //end test part

                    boolean isGoodMsg = true;
                    if(!messageContainsAtt(message, Attribute.name_terminate))
                    {
                        for (String attribute : msgsAttToGet) {
                            if (!messageContainsAtt(message, attribute)) {
                                isGoodMsg = false;
                                break;
                            }
                        }
                    }
                    if (isGoodMsg) {
                        System.out.printf("\ttaking message # %d\n", m);
                        msgsToTake.add(message);
                    } else {
                        System.out.printf("message # %d is not good\n", m);
                    }
                }
            }

            if (msgsToTake.size() > 0) {
                sqs.deleteMessages(url, msgsToTake);
                return msgsToTake;
            }
        }
    }


    private void handleTasks(List<Task> tasks) {

        System.out.println("Debug:: handling tasks");
        logger.log("Debug:: handling tasks");
        for (Task task : tasks) {
            Runnable r = new Runnable() {
                @Override
                public void run() {
                    if (!activeTasks.containsKey(task.taskId)) {
                        File file = new File(task.taskId);
                        s3.getObjectBytes(task.bucket, task.taskId, file);
                        s3.deleteObject(task.bucket, task.taskId);
                        parseTaskFile(task, file);
                        for (Job j : task.getJobs()) {
                            sendJob(j);
                        }
                        file.delete();
                        activeTasks.put(task.taskId, task);
                        calculateWorkers();
                    }
                }
            };
            try {
                System.out.println("Debug:: trying to put task runnable in errands queue");
                logger.log("Debug:: trying to put task runnable in errands queue");
                errandsQ.put(r);
            } catch (Exception e) {
                System.err.println("thread error putting errand\n");
            }
        }
    }

    private void calculateWorkers2() {
        System.out.println("Calculating necessary # of workers");

        String msgNum = sqs.getApproximateNumberOfMessages(Params.jobQueueOut);

        System.out.println("num of messages in JobQueueOut: " + msgNum);
        logger.log("num of messages in JobQueueOut: " + msgNum);
        int i = 2;
        try {
            i = Integer.parseInt(msgNum);
        } catch (NumberFormatException e) {

            System.err.println(i);
        }

        int numOfTasks = i + activeTasks.size();
        int requiredWorkers = (int) Math.ceil((double) numOfTasks / n); // change  '# / n' instead of '# * n'
        int workersToTake = requiredWorkers - activeWorkerIDs.size();
        System.out.println("num of acrive workers: " + activeWorkerIDs.size());

        workersToTake = Math.min(workersToTake, Params.MAX_WORKERS);

        System.out.println("num of workers to add: " + msgNum);
        if (workersToTake > 0) {
            List<String> workerIds = ec2.runInstance(Role.worker, "", workersToTake);
            activeWorkerIDs.addAll(workerIds);
            System.out.println("# of new workers = "+workerIds.size());
        }
        else if (workersToTake < 0) {
            // remove workers
        }
    }

    private void calculateWorkers() {
        System.out.println("Calculating necessary # of workers");

        String msgNum = sqs.getApproximateNumberOfMessages(Params.jobQueueOut);

        double relativeWorkersSum = 0;
        for (Task task : activeTasks.values()) {
            relativeWorkersSum += (double) task.pendingJobs() / task.N;
        }
        int requiredWorkers = (int) Math.ceil(relativeWorkersSum);
        requiredWorkers = Math.min(requiredWorkers, Params.MAX_WORKERS);

        int workersToTake = requiredWorkers - activeWorkerIDs.size();

        System.out.println("num of active workers: " + activeWorkerIDs.size());
        System.out.println("num of required workers: " + requiredWorkers);
        System.out.println("num of workers to add: " + workersToTake);
        if (workersToTake > 0) {

            List<String> workerIds = ec2.runInstance(Role.worker, "", workersToTake);
            activeWorkerIDs.addAll(workerIds);
            System.out.println("# of new workers = "+workerIds.size());
        }
        else if (workersToTake < 0) {
            // remove workers
        }
    }

    private void sendJob(Job j) {
        System.out.println("Sending job "+j.jobId);
        Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
        messageAttributes.put(Attribute.name_jobId, MessageAttributeValue.builder().dataType("String").stringValue(j.jobId).build());
        messageAttributes.put(Attribute.name_taskId, MessageAttributeValue.builder().dataType("String").stringValue(j.taskId).build());
        messageAttributes.put(Attribute.name_jobType, MessageAttributeValue.builder().dataType("String").stringValue(j.type).build());
        String url = sqs.getQueueUrl(Params.jobQueueIn);
        sqs.sendMessage(url, j.url, messageAttributes);
    }

    private void parseTaskFile(Task task, File file) {
        FileReader fileReader = null;
        try {
            fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            int i = 0;
            // Read lines
            String line = "";
            while ((line = bufferedReader.readLine()) != null) {
                i++;
                line = line.trim();
                if (line.length() < 1) {
                    continue;
                }
                String[] words = line.split("\t");
                if (words.length < 2) {
                    continue;
                }
                // add line-job to task
                System.out.printf("type: %s - url: %s\n", words[0], words[1]);
                task.addJob(new Job(task.taskId, i + "_" + task.taskId, words[1], words[0]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void jobListener() {
        while (true) {
            List<Answer> answers = new ArrayList<>();
            String url = sqs.getQueueUrl(Params.jobQueueOut);
            List<String> attsNamesToGet = new ArrayList<>();
            attsNamesToGet.add(Attribute.name_jobId);

            List<String> allAttNames = new ArrayList<>();
            allAttNames.add(Attribute.name_jobId);
            allAttNames.add(Attribute.name_taskId);

            List<Message> messages = waitForMessages(attsNamesToGet, allAttNames, url, 30);

            System.out.println("Debug:: took job-output-messages from queue:\n");
            logger.log("Debug:: took job-output-messages from queue:\n");
            printMessages(messages);

            for (Message msg : messages) {
                if (messageContainsAtt(msg, Attribute.name_jobId)) {
                    answers.add(Answer.fromMessage(msg));
                }
            }
            handleAnswers(answers);
        }
    }

    private void handleAnswers(List<Answer> answers) {
        for (Answer ans : answers) {
            Runnable r = new Runnable() {
                @Override
                public void run() {
                    if (activeTasks.containsKey(ans.taskId)) {
                        System.out.println("received answer for Job "+ans.jobId);
                        Task task = activeTasks.get(ans.taskId);
                        task.updateJob(ans.jobId, ans.content);
                        task.checkTimes();
                        if (task.isReady()) {
                            respondToTask(task);
                        }
                    }
                }
            };
            try {
                errandsQ.put(r);
            } catch (Exception e) {
                System.err.println("thread error putting errand\n");
            }
        }
    }

    private void taskListener() {
        boolean stop = false;
        String url = sqs.getQueueUrl(Params.taskQueueIn);
        String[] attributes = {Attribute.name_taskId, Attribute.name_number, Attribute.name_timeout};
        List<String> attsToGet = new ArrayList<>();
        Collections.addAll(attsToGet, attributes);

        List<String> allAttNames = new ArrayList<>(attsToGet);
        allAttNames.add(Attribute.name_terminate);

        while (!stop) {
            List<Task> tasks = new ArrayList<>();

            List<Message> messages = waitForMessages(attsToGet, allAttNames, url, 30);

            System.out.println("Debug:: took task-messages from queue:\n");
            logger.log("Debug:: took task-messages from queue:\n");
            printMessages(messages);

            for (Message msg : messages) {
                if (messageContainsAtt(msg, Attribute.name_terminate) &&
                        messageGetAttValue(msg, Attribute.name_terminate).equals(Attribute.terminate_value)) {
                    stop = true;
                }
                else{
                    tasks.add(Task.fromMessage(msg));
                }
            }
            handleTasks(tasks);
        }
        // terminate message was received -> stop == true
        setTerminate();
    }

    private void respondToTask(Task task) {
        System.out.println("Responding to task "+task.taskId);
        File file = new File(task.taskId + "_answer");
        try {
            writeAnswer(file, task);
            s3.putObject(Params.taskBucketOut, task.taskId, file);
            file.delete();
            Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
            String queueUrl = sqs.getQueueUrl(Params.taskQueueOut);
            messageAttributes.put(Attribute.name_taskId, MessageAttributeValue.builder().dataType("String").stringValue(task.taskId).build());
            sqs.sendMessage(queueUrl, Params.taskBucketOut, messageAttributes);
        } catch (IOException e) {
            e.printStackTrace();
        }
        activeTasks.remove(task.taskId);
        if (terminate && activeTasks.size() == 0) {
            onTerminate();
        }
    }

    private void writeAnswer(File file, Task task) throws IOException {
        FileWriter writer = new FileWriter(file);

        for (Job j : task.getJobs()) {
            String line = String.format("%s:        %s        %s", j.type, j.url, j.answer);
            writer.write(line + "\r\n");
        }
        writer.flush();
    }

    private void busyWork() {
        while (true) {
            try {
                System.out.println("Trying to take errand from queue");
                Runnable errand = errandsQ.take();
                System.out.println("Running errand");
                errand.run();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    private void setTerminate() {
        this.terminate = true;
        if(activeTasks.size() == 0){
            onTerminate();
        }
    }


    private static String messageGetAttValue(Message msg, String att) {
        String attVal = null;
        if (msg.messageAttributes().containsKey(att)) {
            attVal = msg.messageAttributes().get(att).stringValue();
        }
        return attVal;
    }

    private static boolean messageContainsAtt(Message msg, String att) {
        return msg.messageAttributes().containsKey(att);
    }

    private static void printMessages(List<Message> messages) {
        if (messages != null) {
            System.out.println("Debug:: took task-messages from queue:\n");
            for (Message message :
                    messages) {
                System.out.println("message attributes: ");
                for (String att :
                        message.messageAttributes().keySet()) {
                    MessageAttributeValue value = message.messageAttributes().get(att);
                    System.out.println(att + " : " + value.stringValue());
                }
                System.out.println(message.body());
            }
        }
    }

    private void onTerminate() {
        Runnable r = new Runnable() {
            @Override
            public void run() {
                closeManager();
            }
        };
        try {
            errandsQ.put(r);
        } catch (Exception e) {
            System.err.println("thread error putting errand\n");
        }

    }

    private void closeManager() {
//        close workers
//        purge/close queues
//        close manager
        for (String workerID : activeWorkerIDs) {
            System.out.println("Terminating worker: "+workerID);
            ec2.terminateEC2(workerID);
        }
        sqs.purgeQueue(Params.taskQueueIn);
        sqs.purgeQueue(Params.taskQueueOut);
        sqs.purgeQueue(Params.jobQueueIn);
        sqs.purgeQueue(Params.jobQueueOut);
        String managerID = ec2.findManagersUp();
        ec2.terminateEC2(managerID);

        System.exit(0);
    }




/*

    boolean readMessages2(List<Message> msgs){
        boolean stop = false;
        List<Task> tasks = new ArrayList<Task>();
        if (msgs != null){
            for (Message msg : msgs)
            {
                if (messageContainsAtt(msg, Attribute.name_terminate) &&
                    messageGetAttValue(msg,Attribute.name_terminate).equals(Attribute.terminate_true) ){
                stop = true;
                }
                else{
                    tasks.add(Task.fromMessage(msg));
                }
            }
        }
        handleTasks(tasks);
        return stop;
    }

    private void handleTasks2(List<Task> tasks){
//        System.out.println("Handling tasks");
        logger.log("Handling tasks");
        String queueUrl = sqs.getQueueUrl(Params.taskQueueOut);
        for (Task task : tasks) {
            File file = new File(task.taskId);
            s3.getObjectBytes(task.bucket,task.taskId,file);
            s3.deleteObject(task.bucket,task.taskId);
            s3.putObject(Params.taskBucketOut,task.taskId,file);
//            delete file
            if(file.delete()){
                System.out.println("file deleted");
                logger.log("file deleted");
            }
            else{
                System.out.println("file NOT deleted");
                logger.log("file deleted");
            }
            Map<String,MessageAttributeValue> messageAttributes = new HashMap<>();
            messageAttributes.put(Attribute.name_taskId, MessageAttributeValue.builder().dataType("String").stringValue(task.taskId).build());
            sqs.sendMessage(queueUrl,Params.taskBucketOut,messageAttributes);
        }
    }
*/


}