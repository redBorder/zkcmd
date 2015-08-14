package net.redborder.utils.zkcmd.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

public class ZkUtils {
    final Logger log = LoggerFactory.getLogger(ZkUtils.class);
    CuratorFramework curatorFramework;
    LeaderLatch latch;
    String zkWorkspace;
    String hostname;
    DistributedBarrier barrier;
    ObjectMapper objectMapper;

    public ZkUtils(CuratorFramework curatorFramework, String zkWorkspace) {
        this.curatorFramework = curatorFramework;
        this.zkWorkspace = zkWorkspace;
        this.objectMapper = new ObjectMapper();
        this.latch = new LeaderLatch(curatorFramework, zkWorkspace + "/latch");
        this.barrier = new DistributedBarrier(curatorFramework, zkWorkspace + "/working");


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

    public void incrementTask() {
        String zkPath = zkWorkspace + "/workers/" + hostname;

        try {
            byte[] data = curatorFramework.getData().forPath(zkPath);
            Integer works = new Integer(new String(data));
            works++;
            curatorFramework.setData().forPath(zkPath, works.toString().getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setBarrier() {
        try {
            barrier.waitOnBarrier();
            log.info("Setting barrier. Now, I'm working.");
            barrier.setBarrier();
        } catch (Exception e) {
            log.error("Can't setBarrier");
        }
    }

    public void releaseBarrier() {
        try {
            barrier.removeBarrier();
            log.info("Releasing barrier. Is time to relax.");
        } catch (Exception e) {
            log.error("Can't removeBarrier", hostname);
        }
    }

    public CmdTask getTask() {
        setBarrier();
        CmdTask cmdTask = null;

        try {
            String zkPath = ConfigFile.getInstance().getZkTaskPath();
            List<String> tasks = curatorFramework.getChildren().forPath(zkPath);
            if (!tasks.isEmpty()) {
                String taskName = tasks.get(0);
                byte[] task = curatorFramework.getData().forPath(zkPath + "/" + taskName);
                cmdTask = new CmdTask(objectMapper.readValue(task, Map.class));
                curatorFramework.delete().forPath(zkPath + "/" + taskName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        releaseBarrier();
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
