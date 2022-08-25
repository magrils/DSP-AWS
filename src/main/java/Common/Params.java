package Common;

public class Params {
    // Buckets
    public static final String taskBucketIn = "taskbucketin";
    public static final String taskBucketOut = "taskbucketout";
    public static final String jobBucketOut = "jobbucketout";


    // Queues
    public static final String taskQueueIn = "taskqueuein";
    public static final String taskQueueOut = "taskqueueout";

    public static final String managerControlQueue = "managercontrolqueue";

    public static final String jobQueueIn = "jobqueuein";
    public static final String jobQueueOut = "jobqueueout";

    public static final String workersControlQueue = "workerscontrolqueue";


    public static final String POS = "POS";
    public static final String CONSTITUENCY = "CONSTITUENCY";
    public static final String DEPENDENCY = "DEPENDENCY";
    public static final int Max_Messages = 10;
    public static final int Max_WaitTime = 20;
    public static final int Min_VisibilityTime = 5;
    public static final int MAX_WORKERS = 15;
    public static final String TERMINATE = "terminate";
}
