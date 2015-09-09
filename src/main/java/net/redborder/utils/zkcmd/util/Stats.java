package net.redborder.utils.zkcmd.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Stats extends Thread{
    private final Logger log = LoggerFactory.getLogger(Stats.class);
    volatile boolean running = false;

    AtomicLong jobs;
    AtomicInteger currentJobs;
    long capacity;

    public Stats(){
        this.jobs = new AtomicLong(0L);
        this.currentJobs = new AtomicInteger(0);
        this.capacity = ConfigFile.getInstance().maxTask();
    }


    @Override
    public void run() {
        running = true;
        while (running){
            try {
                log.info("{\"currentJobs\":{}, \"totalCapacity\":{}, \"totalJobs\":{}}", currentJobs.get(), capacity, jobs.get());
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
        this.jobs.incrementAndGet();
        this.currentJobs.incrementAndGet();
    }

    public void decrementJob(){
        this.currentJobs.decrementAndGet();
    }
}
