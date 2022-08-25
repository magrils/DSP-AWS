package Manager;

import Common.Attribute;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.time.LocalDateTime;

public class Job {
    public String taskId;
    public String jobId;
    public LocalDateTime startTime;
    public String url;
    public String type;
    public String answer;
    public boolean isDone;

    public Job(String taskId, String jobId, String url, String type) {
        this.taskId = taskId;
        this.jobId = jobId;
        this.url = url;
        this.type = type;
        this.isDone = false;
        this.startTime = LocalDateTime.now();
    }

    public void checkTimes(int timeout){
        if(isDone){
            return;
        }
        LocalDateTime timeNow = LocalDateTime.now();
        long diffInMinutes = java.time.Duration.between(this.startTime, timeNow).toMinutes();
        if (diffInMinutes>timeout){
            isDone=true;
            answer = "Job timed-out";
        }
    }
}
