package Manager;

import Common.*;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreePrint;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class Main {

    public static void main(String[] args) {
//        int n = Integer.parseInt(args[0]);
        int n = 3;
//        int timeout = Integer.MAX_VALUE;
        System.out.printf("arg[0]: N= %d\n",n);
        myMethod(n);
    }

    private static void myMethod(int n) {
        System.out.println("Hello World! its Sharon in Manager");
        EC2Handler ec2 = new EC2Handler();
        S3Handler s3 = new S3Handler();
        SQSHandler sqs = new SQSHandler();
        Manager manager = new Manager(sqs,s3,ec2, n);

        System.out.println("Waiting for tasks");
        manager.mainManagerWork();
    }










    private static void threadMethods() {
        BlockingQueue<Integer> blockingQueue = new LinkedBlockingDeque<>();
        int n = 8; // Number of threads
        for (int i = 0; i < n; i++) {
            final int j = i;
            Thread object
                    = new Thread(new Runnable() {
                @Override
                public void run() {
                    if(j%2 ==0){
                        try{
                            blockingQueue.put(j);
                        }
                        catch(Exception e){
                            System.err.printf("thread %d error putting int\n",j);
                        }
                    }
                    else{
                        try{
                            int c = blockingQueue.take();
                            System.out.printf("thread %d got int %d\n",j,c);
                        }
                        catch(Exception e){
                            System.err.printf("thread %d error taking int\n",j);
                        }
                    }
                }
            });
            object.start();
        }
    }

    private static void test(){
        System.out.println("Hello World! its Sharon in Manager");
//        EC2Handler ec2 = new EC2Handler();
//        S3Handler s3 = new S3Handler();
//        SQSHandler sqs = new SQSHandler();
//        Manager manager = new Manager(sqs,s3,ec2,5);
//
//        System.out.println("Waiting for tasks");
//        manager.mainManagerWork();



        String line = "CONSTITUENCY\thttps://www.gutenberg.org/files/1659/1659-0.txt";
        String[] words = line.split("\t");
        for (String w
                : words
             ) {
            System.out.println(w);
        }


        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        LocalDateTime dateTime1= LocalDateTime.parse("2022-05-08 22:00:00", formatter);
        LocalDateTime dateTime3= LocalDateTime.parse("2014-11-25 16:00:00", formatter);

        LocalDateTime dateTime2= LocalDateTime.now();
        LocalDateTime dateTime4= LocalDateTime.now();
        long diffInMilli = java.time.Duration.between(dateTime2, dateTime4).toMillis();
        long diffInSeconds = java.time.Duration.between(dateTime2, dateTime4).getSeconds();
        long diffInMinutes = java.time.Duration.between(dateTime2, dateTime4).toMinutes();
        System.out.printf("time in milli %d , seconds %d , mins %d",diffInMilli,diffInSeconds,diffInMinutes);
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

    private static String messageGetAtt(Message msg, String att){
        String attVal = null;
        if (msg.messageAttributes().containsKey(att)) {
            attVal = msg.messageAttributes().get(att).stringValue();
        }
        return attVal;
    }

}
