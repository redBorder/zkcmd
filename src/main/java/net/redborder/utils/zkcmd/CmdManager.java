package net.redborder.utils.zkcmd;

import net.redborder.utils.zkcmd.util.CmdTask;
import net.redborder.utils.zkcmd.util.ConfigFile;
import net.redborder.utils.zkcmd.util.ZkUtils;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CmdManager extends Thread {
    private final Logger log = LoggerFactory.getLogger(CmdManager.class);
    LinkedBlockingQueue<CmdTask> cmdTasks = new LinkedBlockingQueue<>(1000);
    String tmpFilesDir = ConfigFile.getInstance().tmpFilesDir();
    Integer maxTask = ConfigFile.getInstance().maxTask();
    ExecutorService executorService = Executors.newFixedThreadPool(maxTask);
    ZkUtils zkUtils;
    TaskWatcher taskWatcher;

    volatile boolean running = false;

    public CmdManager(ZkUtils zkUtils) {
        this.zkUtils = zkUtils;
        this.taskWatcher = new TaskWatcher();
    }

    private void moreTasks() {
        CmdTask cmdTask = zkUtils.getTask();
        log.info("Looking for new tasks ...");
        if (cmdTask != null) {
            try {
                log.info("Found one task put in the queue!");
                cmdTasks.put(cmdTask);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            String zkPath = ConfigFile.getInstance().getZkTaskPath();
            log.info("No more tasks ... putting watcher at {}", zkPath);
            zkUtils.watcherChildren(taskWatcher, zkPath);
        }
    }

    @Override
    public void run() {
        running = true;
        log.info("Start!");
        File theDir = new File(tmpFilesDir);
        if (!theDir.exists()) {
            theDir.mkdirs();
        }

        while (running) {
            try {
                moreTasks();
                CmdTask cmdTask = cmdTasks.take();
                Map<String, String> files = writteFiles(cmdTask.getFiles());
                String cmd = getCommand(cmdTask.getCmd(), files);

                executorService.submit(new CmdWorker(cmd, files.values()));
                zkUtils.incrementTask();
            } catch (InterruptedException e) {
                log.info("Time to shutdown!");
            }
        }
    }

    public void shutdown() {
        running = false;
        executorService.shutdown();
        try {
            executorService.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        executorService.shutdownNow();
    }

    public String getCommand(String cmd, Map<String, String> files) {
        log.debug("command: " + cmd);
        String command = "";
        for (Map.Entry<String, String> file : files.entrySet()) {
            log.debug("Replace: " + file.getKey() + " to " + file.getValue());
            command = cmd.replace(file.getKey(), file.getValue());
        }
        return command;
    }

    public Map<String, String> writteFiles(Map<String, String> files) {
        Map<String, String> tmpFiles = new HashMap<>();

        for (Map.Entry<String, String> jsonFile : files.entrySet()) {
            try {
                String name = jsonFile.getKey();
                if (!tmpFiles.containsKey(name)) {
                    String content = jsonFile.getValue();
                    String uuid = UUID.randomUUID().toString();
                    File file = new File(tmpFilesDir + File.separator + uuid);
                    if (!file.exists()) {
                        file.createNewFile();
                    }

                    FileWriter fw = new FileWriter(file.getAbsoluteFile());
                    BufferedWriter bw = new BufferedWriter(fw);
                    bw.write(content);
                    bw.close();

                    tmpFiles.put("[[[" + name + "]]]", file.getAbsolutePath());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        return tmpFiles;
    }


    private class TaskWatcher implements CuratorWatcher {

        @Override
        public void process(WatchedEvent watchedEvent) throws Exception {
            Watcher.Event.EventType type = watchedEvent.getType();
            log.info("I'm LEADER!");

            log.info("[WATCH] CmdWatcher :: {} {}" + type.name(), watchedEvent.getPath());

            if (type.equals(Watcher.Event.EventType.NodeChildrenChanged)) {
                moreTasks();
            }
        }
    }
}
