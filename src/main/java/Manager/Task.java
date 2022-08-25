package Manager;

import Common.Attribute;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.util.ArrayList;
import java.util.List;

public class Task {
    public String taskId;
    public String bucket;
    public int N;   // messages per worker
    public int timeout;   // time interval from posting a job until declaring it dead
    public List<Job> getJobs() {
        return jobs;
    }

    private List<Job> jobs;

    public Task( String taskId, String bucket, int N, int timeout) {
        this.taskId = taskId;
        this.bucket = bucket;
        this.jobs = new ArrayList<>();
        this.N = N;
        this.timeout = timeout;
    }


    public static Task fromMessage(Message msg){
        int n = 2;
        int timeout = 17;
        String taskId = null;
        String bucket = msg.body();
        for (String att : msg.messageAttributes().keySet()) {
            MessageAttributeValue value = msg.messageAttributes().get(att);
            if(Attribute.name_taskId.equals(att)){
            taskId = value.stringValue();
            }
            else if(Attribute.name_number.equals(att)){
                try {
                    n = Integer.parseInt(value.stringValue());
                }
                catch (Error e){
                    e.printStackTrace();
                }
            }
            else if(Attribute.name_timeout.equals(att)){
                 try{
                     timeout = Integer.parseInt(value.stringValue());
                 }
                catch (Error e){
                e.printStackTrace();
                }
            }
        }
        return new Task(taskId,bucket, n, timeout);
    }

    public void checkTimes(){
        for (Job j :
                jobs) {
            j.checkTimes(timeout);
        }
    }

    public void addJob(Job j){
        jobs.add(j);
    }

    public boolean isReady(){
        for (Job j :jobs) {
            if (!j.isDone){
                return false;
            }
        }
        return true;
    }

    public int pendingJobs(){
        int i = 0;
        for (Job j : jobs) {
            if (!j.isDone){
                i++;
            }
        }
        return i;
    }

    public void updateJob(String jobId, String content) {
        for (Job j :jobs) {
            if (j.jobId.equals(jobId)){
                j.answer = content;
                j.isDone = true;
            }
        }
    }


}
