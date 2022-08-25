package Worker;

import Common.*;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.trees.*;


import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;

public class Main {


    private static void parseStuff(String parseType, File inFile, File outFile) throws IOException {
        String modelPath = "model/englishPCFG.ser.gz";
        // select parse option
        String optionPOS = "wordsAndTags";
        String optionCON = "penn";
        String optionDEP = "typedDependencies";

        String selectedParseType = optionCON;
        if(parseType.equals(Params.POS)){
            selectedParseType = optionPOS;
        }
        else if(parseType.equals(Params.DEPENDENCY)){
            selectedParseType = optionDEP;
        }

        //setup IO objects
        FileReader fileReader = new FileReader(inFile);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        PrintWriter pw = new PrintWriter(outFile);

        //load parser model
        System.out.println("Loading parser model...");
        LexicalizedParser parser = LexicalizedParser.loadModel(modelPath);
        System.out.println("Loading done!");

        // start parsing line by line, and print output to 'outFile'
        String line = "";
        int i=-1;
        while ((line = bufferedReader.readLine()) != null) {
            i++;
            if(i%20 == 0){
                System.out.printf("parsing the %d th line of the file\n", i);
            }
//            if(i == 60){ //shorter parse runtime for tests
//                break;
//            }
            List<String> words = new ArrayList<>();
            // Insert line to list
            line = line.trim();
            if(line.length()> 0) {
                String[] lineParts = line.split("[ ,.()_/:;\"]+");
                for (String word: lineParts) {
                    if(word.length()>0) {
                        words.add(word);
                    }
                }
                Tree t = parser.parseStrings(words);
                TreePrint tp = new TreePrint(selectedParseType);
                tp.printTree(t, pw);
            }
        }
    }

    private static void doWork(S3Handler s3, SQSHandler sqs ){

        System.out.println("Waiting for Job");
        Message msg = waitForJob(sqs);
        // read message & parameters
        String taskId = messageGetAttValue(msg, Attribute.name_taskId);
        String jobId = messageGetAttValue(msg, Attribute.name_jobId);
        String jobType = messageGetAttValue(msg, Attribute.name_jobType);
        String textUrl = msg.body();

        File inFile = null;
        File outFile = null;
        boolean success = false;
        String answer = "unable to parse file";

        // if parameters are valid
        if(taskId != null && jobId != null && jobType != null){
            String inFilePath = jobId+"_inFile.txt";
            inFile = new File(inFilePath);
            outFile = new File(jobId+"_outFile.txt");

            try {
                downloadFile(textUrl,inFilePath);
                parseStuff(jobType,inFile, outFile);
                success = true;
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        if(success){
            // upload parsing result
            s3.putObject(Params.jobBucketOut,jobId,outFile);
            answer = s3.getObjectUrl(Params.jobBucketOut,jobId);

            //delete local files
            if(inFile.delete())
                System.out.println("inFile deleted successfully");
            if(outFile.delete())
                System.out.println("outFile deleted successfully");;
        }
        System.out.println("Sending job result message");
        sendCompleteJobMessage(sqs,answer,jobId,taskId);

        //delete original job message
        List<Message> msgsToDelete = new ArrayList<>();
        msgsToDelete.add(msg);
        sqs.deleteMessages(Params.jobQueueIn, msgsToDelete);


    }

    private static void sendCompleteJobMessage(SQSHandler sqs, String answer, String jobId, String taskId){
        Map<String,MessageAttributeValue> messageAttributes = new HashMap<>();
        messageAttributes.put(Attribute.name_taskId, MessageAttributeValue.builder().dataType("String").stringValue(taskId).build());
        messageAttributes.put(Attribute.name_jobId, MessageAttributeValue.builder().dataType("String").stringValue(jobId).build());
        String url = sqs.getQueueUrl(Params.jobQueueOut);
        sqs.sendMessage(url,answer, messageAttributes);
    }

    private static Message waitForJob(SQSHandler sqs){
        String url = sqs.getQueueUrl(Params.jobQueueIn);
        List<String> attNames = new ArrayList<String>();
        attNames.add(Attribute.name_taskId);
        attNames.add(Attribute.name_jobId);
        attNames.add(Attribute.name_jobType);
        while(true){
            List<Message> messages = sqs.receiveMessages(url,attNames,900,Params.Max_WaitTime,1);
            if (messages!=null){
                for (Message message : messages) {
//                    List<Message> msgsToDelete = new ArrayList<>();
                    for (String att : message.messageAttributes().keySet()) {
                        MessageAttributeValue value = message.messageAttributes().get(att);
                        if(Attribute.name_jobId.equals(att)){
//                            msgsToDelete.add(message);
                            return message;
                        }
                    }
                }
            }
        }
    }

    private static String messageGetAttValue(Message msg, String att){
        String attVal = null;
        if (msg.messageAttributes().containsKey(att)) {
            attVal = msg.messageAttributes().get(att).stringValue();
        }
        return attVal;
    }


    private static void downloadFile(String url, String filePath) throws IOException {
        InputStream in = new URL(url).openStream();
        Files.copy(in, Paths.get(filePath), StandardCopyOption.REPLACE_EXISTING);
    }

    public static void main(String[] args) {
        S3Handler s3 = new S3Handler();
        SQSHandler sqs = new SQSHandler();

        while (true)
        {
            doWork(s3,sqs);
        }
    }

}
