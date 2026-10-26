
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
public class JobScheduler{
    int workers;
    List<Thread> threads = new ArrayList<>();
    volatile boolean shutdown;
    PriorityQueue<Job> scheduledJobs;
    ConcurrentHashMap<String, Job> table;
    ReentrantLock lock = new ReentrantLock();
    Condition changed = lock.newCondition();

    public JobScheduler(int workers){
        this.workers = workers;
        scheduledJobs = new PriorityQueue<>(Comparator.comparingLong((Job t) -> t.runAt));
        table = new ConcurrentHashMap<>();

    }

public void schedule(Job job){
    
    lock.lock();
    try{
        if(shutdown){
        throw new IllegalStateException("Scheduler is shut down");
    }
    table.put(job.id,job);
    scheduledJobs.add(job);
    changed.signalAll();
    }finally{
        lock.unlock();
    } 
}

private void workerLoop(){
    while(!shutdown){
        Job job = null;
        lock.lock();
        try {
            while(scheduledJobs.isEmpty()) changed.await();

            Job head = scheduledJobs.peek();
            long delay = head.runAt - System.currentTimeMillis();

            if(delay > 0){
                changed.awaitNanos( delay * 1000000);
                continue;
                
            }
            job = scheduledJobs.poll();

        } catch (InterruptedException e) {
            return;
        }finally{
            lock.unlock();
        }

        if(job.status == JobStatus.CANCELLED) continue;
        job.status = JobStatus.RUNNING;
        try{
        job.task.run();
        if(job.status == JobStatus.RUNNING){
        if(job.intervalMillis > 0 ){
            job.runAt += job.intervalMillis;
            job.status = JobStatus.SCHEDULED;
            lock.lock();
            try{
            scheduledJobs.add(job);
            changed.signalAll();
            }finally{lock.unlock();}
        }else{
            job.status = JobStatus.COMPLETED;
        }
        }
        }catch(Throwable t){
            job.status = JobStatus.FAILED;
        }


    }
}

public void start(){
    
    for(int i = 0;i<workers;i++){
        Thread t = new Thread(this::workerLoop,"Thread-Name-is-"+i);
        threads.add(t);
        t.start();
    }
}

public boolean cancel(String id){
    Job j = table.get(id);
    if(j == null) return false;
    j.status = JobStatus.CANCELLED;
    return true;
}

public JobStatus status(String id){
    Job j = table.get(id);
    return j == null ? null : j.status;

}
public void shutdown(){
     shutdown = true;
     lock.lock();
     try {
         changed.signalAll();
     } finally {
        lock.unlock();
     }

     for(Thread t : threads){
        t.interrupt();
     }
}
}