package net.redborder.utils.zkcmd.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Stats extends Thread{
    private final Logger log = LoggerFactory.getLogger(Stats.class);
    volatile boolean running = false;

    long jobs;
    Integer currentJobs;
    long capacity;

    public Stats(){
        this.jobs = 0L;
        this.currentJobs = 0;
        this.capacity = ConfigFile.getInstance().maxTask();
    }


    @Override
    public void run() {
        running = true;
        while (running){
            try {
                log.info("{\"currentJobs\":{}, \"totalCapacity\":{}, \"totalJobs\":{}}", currentJobs, capacity, jobs);
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void close(){
        running = false;
    }

    public void incrementJob(){
        this.jobs ++;
    }

    public void setCurrentJobs(Integer currentJobs){
        this.currentJobs = currentJobs;
    }
}
