package LocalApp;

import Common.*;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Main {


    public static void main(String[] args) {

        printTime();

//        tests();
//        tearDown();
//        setUpQandB();
//        helpReadQueue(Params.taskQueueIn);
//        helpReadQueue(Params.taskQueueOut);

        prerun(args);
    }

    private static void prerun(String[] args) {
        String inputPath = args[0];
        String outPath = args[1];
        String n = args[2];
        boolean terminate = false;
        if (args.length>3 && args[3].toLowerCase().equals("terminate")){
            terminate = true;
        }
        String timeout = "";
        if (args.length == 5){
            timeout = args[4];
        }

        try {
            Integer.parseInt(args[2]);
            if (args.length == 5)
                Integer.parseInt(args[4]);
        }
        catch (NumberFormatException e){
            System.err.println("3rd argument (N) must be an integer");
            System.err.println("5rd argument (timeout - in minutes) must be an integer");
            System.exit(-5);
        }
        run(inputPath, outPath, n, timeout, terminate);
    }

    private static void run(String inputPath, String outPath, String n, String timeout, boolean terminate) {
        EC2Handler ec2 = new EC2Handler();
        S3Handler s3 = new S3Handler();
        SQSHandler sqs = new SQSHandler();

        // find manager node
        String managerId = ec2.findManagersUp();
        if (managerId == null) {
//        if (false) { // for testing with local manager
                System.out.println("Starting new Manager node");
            managerId = ec2.runInstance(Role.manager, timeout,1).get(0);
        }
        else{
            System.out.println("A Manager node is already running!!");
        }

        // upload input file
        String bucket = Params.taskBucketIn;
        String taskId = "task" + System.nanoTime();
        File inFile = new File(inputPath);
        s3.putObject(bucket, taskId, inFile);

        System.out.println("Sending task message: "+taskId);
        sendTaskMessage(sqs,bucket,taskId, n, timeout);


        System.out.println("Waiting for answer");
        // sqs. wait for outcome on queue Params.taskQueueOut
        Message msg = waitForResponse(sqs, taskId);

        System.out.println("Reading answer message");
        // read message
        if (!Params.taskBucketOut.equals(msg.body())){
            System.exit(5);
        }

        System.out.println("Downloading answer file");
        //download file
        String tempPath = "temp.txt";
        File tempFile = new File(tempPath);
        s3.getObjectBytes(Params.taskBucketOut, taskId, tempFile);
        s3.deleteObject(Params.taskBucketOut, taskId);
        File outFile = new File(outPath);


        System.out.println("Creating html file");
        try {
            createHTML(tempFile, outFile,taskId);
        } catch (IOException e) {
            System.err.println("Could not read/write HTML file");
            System.exit(5);
        }

        // delete answer message
        sqs.deleteMessage(Params.taskQueueOut, msg);

        //task519046246905700
//        //terminate
        if(terminate){
            System.out.println("terminate");
            sendTerminateMessage(sqs);
        }
    }

    private static void sendTaskMessage(SQSHandler sqs, String bucket, String key, String N, String timeout){
        Map<String,MessageAttributeValue> messageAttributes = new HashMap<>();
        messageAttributes.put(Attribute.name_taskId, MessageAttributeValue.builder().dataType("String").stringValue(key).build());
        messageAttributes.put(Attribute.name_number, MessageAttributeValue.builder().dataType("String").stringValue(N).build());
        messageAttributes.put(Attribute.name_timeout, MessageAttributeValue.builder().dataType("String").stringValue(timeout).build());
        String url = sqs.getQueueUrl(Params.taskQueueIn);
        sqs.sendMessage(url,bucket, messageAttributes);
    }


    private static void sendTerminateMessage(SQSHandler sqs){
        Map<String,MessageAttributeValue> messageAttributes = new HashMap<>();
        messageAttributes.put(Attribute.name_terminate, MessageAttributeValue.builder().dataType("String").stringValue(Attribute.terminate_value).build());
        String url = sqs.getQueueUrl(Params.taskQueueIn);
        sqs.sendMessage(url,Params.TERMINATE, messageAttributes);
    }


    private static Message waitForResponse(SQSHandler sqs,String taskId){
        System.out.println("Waiting for a response");
        String url = sqs.getQueueUrl(Params.taskQueueOut);
        List<String> attNames = new ArrayList<String>();
        attNames.add(Attribute.name_taskId);
        while(true){
            List<Message> messages = sqs.receiveMessages(url,attNames,Params.Min_VisibilityTime,Params.Max_WaitTime,Params.Max_Messages);
            if (messages!=null){
                for (Message message : messages) {
                    for (String att : message.messageAttributes().keySet()) {
                        MessageAttributeValue value = message.messageAttributes().get(att);
                        System.out.printf("Debug:: attribute - %s , %s\n",att,value.stringValue());
                        if(Attribute.name_taskId.equals(att) && taskId.equals(value.stringValue())){
                            List<Message> msgToDelete = new ArrayList<>();
                            msgToDelete.add(message);
                            System.out.printf("Debug:: messages to delete  - # %d \n",msgToDelete.size());
                            sqs.deleteMessages(url, msgToDelete);
                            return message;
                        }
                    }
                }
            }
        }
    }

    private static void createHTML(File tempFile, File outFile,String taskid) throws IOException {

        String header =
                "<!DOCTYPE html>\n" +
                "<html>\n" +
                "<head>\n" +
                "<title>DSP Server</title>\n" +
                "</head>\n" +
                "<body>\n" +
                "\n" +
                "<h1>Parsed Docs</h1>\n" +
                "\n" +
                "<ul>\n";
        String ending =
                "</ul>\n" +
                "\n" +
                "<p> taskID: "+ taskid +" </p>\n" +
                "\n" +
                "</body>\n" +
                "</html>";

        FileWriter writer = new FileWriter(outFile);
        FileReader fileReader = new FileReader(tempFile);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        //write header
        writer.write(header);

        // Read lines
        String line = "";
        while ((line = bufferedReader.readLine()) != null) {
            // Insert line to list
            line = line.trim();
            if (line.length()>0){
                line = "<li>" + line + "</li>";
                // Write new line to new file
                writer.write(line + "\r\n");
            }
        }

        // write ending
        writer.write(ending);

        // Close reader and writer
        bufferedReader.close();
        writer.close();

        tempFile.delete();
    }
    private static void printMessages(List<Message> messages){
        if (messages != null){
            System.out.println("received "+messages.size()+" new messages");
            for (Message message :
                    messages) {
                System.out.println("message attributes: ");
                for (String att :
                        message.messageAttributes().keySet()) {
                    MessageAttributeValue value = message.messageAttributes().get(att);
                    System.out.println(att+" : "+value.stringValue());
                }
                System.out.println(message.body());
            }
        }
    }

    private static void printTime(){
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        System.out.println(dtf.format(now));
    }

    private static void helpReadQueue(String qName){
        int count = 0;
        System.out.printf("Printing messages form q %s\n", qName);
        List<String> attNames = new ArrayList<>();
        attNames.add(Attribute.name_taskId);
        attNames.add(Attribute.name_number);


        SQSHandler sqs = new SQSHandler();
        String queueUrl = sqs.getQueueUrl(qName);
        do {
            List<Message> messages = sqs.receiveMessages(queueUrl, attNames, 2, 8, 10);
            count = messages.size();
            // print messages content
            printMessages(messages);

            // delete messages
            if (messages != null && messages.size() > 0){
                sqs.deleteMessages(queueUrl,messages);
            }
        }
        while(count>0);

        sqs.close();

    }




    // TESTS
    private static void setUpQandB(){

        S3Handler s3 = new S3Handler();
//        s3.createBucket(Params.jobBucketOut);
        s3.createBucket(Params.taskBucketIn);
        s3.createBucket(Params.taskBucketOut);

        s3.close();


//        SQSHandler sqs = new SQSHandler();
//        String queueUrl1= sqs.createQueue(Params.jobQueueIn);
//        String queueUrl2= sqs.createQueue(Params.jobQueueOut);
//        String queueUrl3= sqs.createQueue(Params.taskQueueIn);
//        String queueUrl4= sqs.createQueue(Params.taskQueueOut);
//        sqs.close();
    }

    private static void tearDown(){

        SQSHandler sqs = new SQSHandler();
//        String queueUrl= sqs.getQueueUrl(Params.managerControlQueue);
//        sqs.deleteQueue(queueUrl);
        sqs.close();
        // wait 60 seconds
    }

    private static void tests(){
        printTime();

//        testS3();
//        testSQS();
        testEC2();

    }

    private static void testS3() {
        System.out.println("Hello World! its Sharon in LOCAL APP");

        String bucket = "dspmybucket3";
        String key = "example3";

        String inputPath = "C:\\Users\\Dell\\Documents\\YEAR4\\DSP\\task1\\task1\\storage\\input3.txt";
        File fIn = new File(inputPath);
        String outPath = "C:\\Users\\Dell\\Documents\\YEAR4\\DSP\\task1\\task1\\storage\\myoutput3.txt";
        File fOut = new File(outPath);


        S3Handler s3 = new S3Handler();

        bucket = "dsptestbucket5";
//        s3.createBucket(bucket);
        s3.putObject(bucket, key, fIn);
        s3.getObjectBytes(bucket, key, fOut);
        s3.listObjects(bucket);
        s3.deleteObject(bucket,key);
//        s3.deleteBucket(bucket);
        s3.close();
    }

    private static void testSQS(){

//        String queueName = "queue" + System.currentTimeMillis();

        String queueName = "queuemytest";
        String msgAtt1 = "att1";
        String msgAtt2 = "att2";
        String messageBody = "Hello world! its me";
        Map<String,MessageAttributeValue> messageAttributes = new HashMap<>();
        messageAttributes.put(msgAtt1, MessageAttributeValue.builder().dataType("String").stringValue("$att stuff goes here$").build());
        messageAttributes.put(msgAtt2, MessageAttributeValue.builder().dataType("String").stringValue("$more stuff goes here$").build());
//        SqsClient sqsClient = SqsClient.builder()
//                .region(region)
//                .build();

        SQSHandler sqs = new SQSHandler();
        // Perform various tasks on the Amazon SQS queue

//        String queueUrl= sqs.createQueue(queueName);
        String queueUrl = sqs.getQueueUrl(queueName);

        System.out.println("url: "+queueUrl);

//        sqs.sendMessage(queueUrl, messageBody, messageAttributes);

        List<Message> messages = sqs.receiveMessages(queueUrl, messageAttributes.keySet(), 2, 0, 10);

        // print messages content
        printMessages(messages);

        // delete messages
        if (messages != null && messages.size() > 0){
            sqs.deleteMessages(queueUrl,messages);
        }

//        deleteQueue(sqsClient,queueUrl);
        sqs.close();
    }

    private static void testEC2(){
        EC2Handler ec2 = new EC2Handler();

//        String instId = ec2.runInstance(Role.worker);
//
//        instId = ec2.runInstance(Role.manager);

//        System.out.println("instance Initializing");
//        System.out.println("instance id - "+instId+"\n");

        ec2.findManagersUp();
//        ec2.describeTags(instId);
//        ec2.findRunningEC2Instances();

        ec2.close();

    }

}
