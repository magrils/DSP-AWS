package Common;

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class SQSHandler {

    private SqsClient sqsClient;
    private Region region;

    public SQSHandler() {
        region = Region.US_EAST_1;
        this.sqsClient = SqsClient.builder()
                .region(region)
                .build();
    }
    public void close(){
        sqsClient.close();
    }

    public String createQueue(String queueName) {
        System.out.println("Creating Queue '"+queueName+"':");
        try {
            // snippet-start:[sqs.java2.sqs_example.create_queue]

            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();

            sqsClient.createQueue(createQueueRequest);
            // snippet-end:[sqs.java2.sqs_example.create_queue]


            // snippet-start:[sqs.java2.sqs_example.get_queue]
            GetQueueUrlResponse getQueueUrlResponse =
                    sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            String queueUrl = getQueueUrlResponse.queueUrl();

            System.out.println("\nGot queue url: "+queueUrl);
            return queueUrl;

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
//            System.exit(5);
        }
        System.out.println("Done");
        return null;
    }

    public String getQueueUrl(String queueName){
        try{
            GetQueueUrlResponse getQueueUrlResponse =
                    sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            String queueUrl = getQueueUrlResponse.queueUrl();
            return queueUrl;

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(5);
        }
        return null;
    }

    public String deleteQueue(String queueUrl ) {

        try {
            System.out.println("\nSQS - Deleting Queue '"+queueUrl+"':");

            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder().queueUrl(queueUrl).build();

            DeleteQueueResponse response = sqsClient.deleteQueue(deleteQueueRequest);


            System.out.println("\nSQS - Queue deleted");

        } catch (SdkClientException e) {
            e.printStackTrace();
            System.err.println("Client side Error");
            System.exit(5);
        }
        catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(5);
        }

        System.out.println("Done");
        return "";
        // snippet-end:[sqs.java2.sqs_example.get_queue]
    }

    public void purgeQueue(String queueUrl ) {

        try {
            System.out.println("\nSQS - Deleting Queue '"+queueUrl+"':");

            PurgeQueueRequest purgeQueueRequest = PurgeQueueRequest.builder().queueUrl(queueUrl).build();

            PurgeQueueResponse response = sqsClient.purgeQueue(purgeQueueRequest);
            System.out.println("\nSQS - Queue deleted");

        } catch (SdkClientException e) {
            e.printStackTrace();
            System.err.println("Client side Error");
            System.exit(5);
        }
        catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        System.out.println("Done");
    }


    public void myListQueues() {

        System.out.println("\nSQS - List Queues");
        // snippet-start:[sqs.java2.sqs_example.list_queues]
        try {
            ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder().build();
            ListQueuesResponse listQueuesResponse = sqsClient.listQueues(listQueuesRequest);

            for (String url : listQueuesResponse.queueUrls()) {
                System.out.println(url);
            }

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(5);
        }
        // snippet-end:[sqs.java2.sqs_example.list_queues]
    }


    public void sendMessage(String queueUrl,String body, Map<String,MessageAttributeValue> messageAttributes){
        System.out.println("SQS - Sending message to queue '"+queueUrl+"':");
        try {
            SendMessageRequest.Builder req = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(body);
            if (messageAttributes!=null){
                    req.messageAttributes(messageAttributes);
            }
            sqsClient.sendMessage(req.build());

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(5);
        }
        System.out.println("Done");
    }

    public void sendBatchMessages(String queueUrl) {

        System.out.println("\nSQS - Send multiple messages");

        try {
            // snippet-start:[sqs.java2.sqs_example.send__multiple_messages]
            SendMessageBatchRequest sendMessageBatchRequest = SendMessageBatchRequest.builder()
                    .queueUrl(queueUrl)
                    .entries(SendMessageBatchRequestEntry.builder().id("id1").messageBody("Hello from msg 1").build(),
                            SendMessageBatchRequestEntry.builder().id("id2").messageBody("msg 2").delaySeconds(10).build())
                    .build();
            sqsClient.sendMessageBatch(sendMessageBatchRequest);
            // snippet-end:[sqs.java2.sqs_example.send__multiple_messages]

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(5);
        }
    }

    public List<Message> receiveMessages(String queueUrl, Collection<String> msgAttributeNames, int visibilityTimeout,int waitTime, int maxMessages) {

        System.out.println("\nSQS - Receive messages form "+queueUrl);

        try {
            // snippet-start:[sqs.java2.sqs_example.retrieve_messages]
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .visibilityTimeout(visibilityTimeout)
                    .maxNumberOfMessages(maxMessages)
                    .waitTimeSeconds(waitTime)
                    .messageAttributeNames(msgAttributeNames)
                    .build();
            List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();

            System.out.printf("SQS - Done with %d messages\n",messages.size());
            return messages;
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(5);
        }
        return null;
    }

    public void changeMessages(String queueUrl, List<Message> messages) {

        System.out.println("\nSQS - Change Message Visibility");

        try {

            for (Message message : messages) {
                ChangeMessageVisibilityRequest req = ChangeMessageVisibilityRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(message.receiptHandle())
                        .visibilityTimeout(100)
                        .build();
                sqsClient.changeMessageVisibility(req);
            }
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(5);
        }
    }

    public void deleteMessages(String queueUrl,  List<Message> messages) {
        System.out.println("\nSQS - Delete Messages");
        // snippet-start:[sqs.java2.sqs_example.delete_message]

        try {
            for (Message message : messages) {
                DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(message.receiptHandle())
                        .build();
                sqsClient.deleteMessage(deleteMessageRequest);
            }
            // snippet-end:[sqs.java2.sqs_example.delete_message]

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(5);
        }
        System.out.println("Done");
    }

    public void deleteMessage(String queueUrl,  Message message) {
        System.out.println("\nSQS - Delete Messages");

        try {
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
            sqsClient.deleteMessage(deleteMessageRequest);
        }
        catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(5);
        }
        System.out.println("Done");
    }

    public void getAttributes( String queueName) {

        try {
            GetQueueUrlResponse getQueueUrlResponse =
                    sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());

            String queueUrl = getQueueUrlResponse.queueUrl();


            // Specify the attributes to retrieve.
            List<QueueAttributeName> atts = new ArrayList<>();
            atts.add(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES);

            GetQueueAttributesRequest attributesRequest= GetQueueAttributesRequest.builder()
                    .queueUrl(queueUrl)
                    .attributeNames(atts)
                    .build();

            GetQueueAttributesResponse response = sqsClient.getQueueAttributes(attributesRequest);

            Map<String,String> queueAtts = response.attributesAsStrings();
            for (Map.Entry<String,String> queueAtt : queueAtts.entrySet())
                System.out.println("Key = " + queueAtt.getKey() +
                        ", Value = " + queueAtt.getValue());

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
    public String getApproximateNumberOfMessages( String queueName) {

        try {
            GetQueueUrlResponse getQueueUrlResponse =
                    sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());

            String queueUrl = getQueueUrlResponse.queueUrl();


            // Specify the attributes to retrieve.
            List<QueueAttributeName> atts = new ArrayList<>();
            atts.add(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES);

            GetQueueAttributesRequest attributesRequest= GetQueueAttributesRequest.builder()
                    .queueUrl(queueUrl)
                    .attributeNames(atts)
                    .build();

            GetQueueAttributesResponse response = sqsClient.getQueueAttributes(attributesRequest);

            Map<String,String> queueAtts = response.attributesAsStrings();
            return queueAtts.getOrDefault("ApproximateNumberOfMessages", "");

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }

}
