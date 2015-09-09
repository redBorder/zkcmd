package net.redborder.utils.zkcmd.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ZkUtils {
    final Logger log = LoggerFactory.getLogger(ZkUtils.class);
    CuratorFramework curatorFramework;
    LeaderLatch latch;
    String zkWorkspace;
    String hostname;
    InterProcessMutex mutex;
    ObjectMapper objectMapper;

    public ZkUtils(CuratorFramework curatorFramework, String zkWorkspace) {
        this.curatorFramework = curatorFramework;
        this.zkWorkspace = zkWorkspace;
        this.objectMapper = new ObjectMapper();
        this.latch = new LeaderLatch(curatorFramework, zkWorkspace + "/latch");
        this.mutex = new InterProcessMutex(curatorFramework, zkWorkspace + "/working");


        try {
            this.hostname = InetAddress.getLocalHost().getHostName();
            latch.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean isLeader() {
        return latch.hasLeadership();
    }

    public boolean checkExist(String zkPath) {
        boolean exist = false;
        try {
            exist = curatorFramework.checkExists().forPath(zkPath) != null;
        } catch (Exception e) {
            log.error("Can't register this node {}", hostname);
        }

        return exist;
    }

    public void registerNode() {
        try {
            String zkPath = zkWorkspace + "/workers/" + hostname;

            if (!checkExist(zkPath)) {
                curatorFramework.create().creatingParentsIfNeeded().forPath(zkPath, "0".getBytes());
            }

        } catch (Exception e) {
            log.error("Can't register this node {}", hostname);
        }
    }

    public void unRegisterNode() {
        try {
            String zkPath = zkWorkspace + "/workers/" + hostname;

            if (checkExist(zkPath)) {
                curatorFramework.delete().forPath(zkPath);
            }

        } catch (Exception e) {
            log.error("Can't unregister this node {}", hostname);
        }
    }

    public Integer incrementTask() {
        String zkPath = zkWorkspace + "/workers/" + hostname;
        Integer works = 0;
        try {
            byte[] data = curatorFramework.getData().forPath(zkPath);
            works = new Integer(new String(data));
            works++;
            curatorFramework.setData().forPath(zkPath, works.toString().getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }

        return works;
    }

    public void setMutex() {
        try {
            log.info("Waiting on mutex. Other node is working ...");
            mutex.acquire();
            log.info("Setting mutex. Now, I'm working.");
        } catch (Exception e) {
            log.error("Can't setMutex");
        }
    }

    public void releaseMutex() {
        try {
            mutex.release();
            log.info("Releasing mutex. Is time to relax.");
        } catch (Exception e) {
            log.error("Can't releaseMutex", hostname);
        }
    }

    public CmdTask getTask() {
        setMutex();
        CmdTask cmdTask = null;
        List<String> tasks = new ArrayList<>();
        String zkPath = ConfigFile.getInstance().getZkTaskPath();

        try {
            tasks = curatorFramework.getChildren().forPath(zkPath);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        String taskName = "";
        if (!tasks.isEmpty()) {
            try {
                taskName = tasks.get(0);
                byte[] task = curatorFramework.getData().forPath(zkPath + "/" + taskName);
                cmdTask = new CmdTask(objectMapper.readValue(task, Map.class));
                curatorFramework.delete().forPath(zkPath + "/" + taskName);
            } catch (Exception e) {
                log.warn("Task {} doesn't exist try to get other task ...", taskName);

            }
        }

        releaseMutex();

        return cmdTask;
    }

    public void watcherChildren(CuratorWatcher watcher, String zkPath) {
        try {
            curatorFramework.getChildren().usingWatcher(watcher).forPath(zkPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
