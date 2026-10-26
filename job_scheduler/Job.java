public class Job{
    String id;
    Runnable task;
    long runAt;
    long intervalMillis;
    volatile JobStatus status;

    public Job(String id, Runnable task, long runAt, long intervalMilli){
        this.id = id;
        this.task = task;
        this.runAt = runAt;
        this.intervalMillis = intervalMilli;
        this.status = JobStatus.SCHEDULED;
    }


}