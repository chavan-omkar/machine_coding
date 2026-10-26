
public class Main{
    public static void main(String[] args) throws InterruptedException
    {
        JobScheduler scheduler = new JobScheduler(3);
        long now = System.currentTimeMillis();
        scheduler.start();

            scheduler.schedule(
            new Job("ONCE", () -> log(now,"once ran"), now + 3000,0));

            //  scheduler.schedule(
            // new Job("B", () -> System.out.println(Thread.currentThread().getName() +" B"), now + 3000,0));

            //  scheduler.schedule(
            // new Job("C", () -> System.out.println(Thread.currentThread().getName() +" C"), now + 3000,0));


            scheduler.schedule(
            new Job("TICK", () -> log(now,"tick"), now + 500,1000));

            scheduler.schedule(new Job("BOOM",() -> {throw new RuntimeException("boom");},now + 1500,0));


            for(String id : new String[]{"P1","P2","P3"}){
                scheduler.schedule(new Job(id,()-> {
                    log(now,id + " start");
                    
                    try{Thread.sleep(1000);
                    }catch(InterruptedException e){
                        log(now,id + " end");
                    }
                    
                    },now + 2000,0));
            }



            Thread.sleep(3500);

            System.out.println("Tick Job Cancelled :- " + scheduler.cancel("TICK"));
            System.out.println("Nope Job Cancelled :- " + scheduler.cancel("Nope"));

            Thread.sleep(1500);

            System.out.println(scheduler.status("TICK"));
            System.out.println(scheduler.status("Nope"));

            scheduler.shutdown();

            try {
                scheduler.schedule(new Job("LATE",()->log(System.currentTimeMillis(),"Late Job"),now + 1400,0));

            } catch (IllegalStateException e) {
                System.out.println(e.getMessage());
            }


            // scheduler.start();

            // while(!scheduler.scheduledJobs.isEmpty()){
            //     Job j = scheduler.scheduledJobs.poll();

            //     long delay = j.runAt - System.currentTimeMillis();
            //      if(delay > 0){
            //         try{
            //         Thread.sleep(delay);
            //         }catch(InterruptedException e){

            //         }
            //      }

            //      j.task.run();

            // }
    }

    private static void log(long start, String msg){
        System.out.println((System.currentTimeMillis()-start) +"ms " + Thread.currentThread().getName() + " " + msg);
    }
}