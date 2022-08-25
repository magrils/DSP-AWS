package Manager;

import Common.Attribute;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

public class Answer {
    public String taskId;
    public String jobId;
    public String content;


    public Answer(String taskId, String jobId, String content) {
        this.taskId = taskId;
        this.jobId = jobId;
        this.content = content;
    }

    public static Answer fromMessage(Message msg){
        int n = -1;
        String taskId = null;
        String jobId = null;
        String content = msg.body();
        for (String att : msg.messageAttributes().keySet()) {
            MessageAttributeValue value = msg.messageAttributes().get(att);
            if(Attribute.name_taskId.equals(att)){
                taskId = value.stringValue();
            }
            else if(Attribute.name_jobId.equals(att)){
                jobId = value.stringValue();
            }
        }
        return new Answer(taskId,jobId,content);
    }
}
