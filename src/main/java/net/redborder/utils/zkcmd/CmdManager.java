package net.redborder.utils.zkcmd;

import net.redborder.clusterizer.Task;
import net.redborder.clusterizer.TasksChangedListener;
import net.redborder.utils.zkcmd.util.CmdTask;
import net.redborder.utils.zkcmd.util.ConfigFile;
import org.apache.curator.framework.CuratorFramework;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CmdManager extends Thread implements TasksChangedListener {
    private final Logger log = LoggerFactory.getLogger(CmdManager.class);
    LinkedBlockingQueue<CmdTask> cmdTasks = new LinkedBlockingQueue<>(200);
    ObjectMapper objectMapper = new ObjectMapper();
    String tmpFilesDir = ConfigFile.getInstance().tmpFilesDir();
    Integer maxTask = ConfigFile.getInstance().maxTask();
    ExecutorService executorService = Executors.newFixedThreadPool(maxTask);
    CuratorFramework curatorFramework;

    volatile boolean running = false;

    public CmdManager(CuratorFramework curatorFramework) {
        this.curatorFramework = curatorFramework;
    }

    public void updateTasks(List<Task> list) {
        if (!list.isEmpty()) {
            try {
                String hostname = InetAddress.getLocalHost().getHostName();
                curatorFramework.setData().forPath("/rb_zkcmd/clusterizer/tasks/" + hostname, "[]".getBytes());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        for (Task t : list) {
            CmdTask cmdTask = new CmdTask(t.asMap());
            try {
                cmdTasks.put(cmdTask);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
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
                CmdTask cmdTask = cmdTasks.take();
                Map<String, String> files = writteFiles(cmdTask.getFiles());
                String cmd = getCommand(cmdTask.getCmd(), files);

                executorService.submit(new CmdWorker(cmd, files.values()));
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
}
