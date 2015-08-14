package net.redborder.utils.zkcmd;

import net.redborder.clusterizer.Task;
import net.redborder.clusterizer.ZkTasksHandler;
import net.redborder.utils.zkcmd.util.CmdTask;
import net.redborder.utils.zkcmd.util.ConfigFile;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class CmdWatcher implements CuratorWatcher {
    final Logger log = LoggerFactory.getLogger(CmdWatcher.class);
    ZkTasksHandler zkTasksHandler;
    CuratorFramework curatorFramework;
    String zkTaskPath;
    ObjectMapper objectMapper = new ObjectMapper();


    public CmdWatcher(ZkTasksHandler zkTasksHandler, String zkTaskPath) {
        log.debug("Created new CmdWatcher!");
        this.zkTasksHandler = zkTasksHandler;
        this.curatorFramework = zkTasksHandler.getCuratorClient();
        this.zkTaskPath = zkTaskPath;
    }

    public void process(WatchedEvent watchedEvent) throws Exception {
        if (zkTasksHandler.isLeader()) {
            Watcher.Event.EventType type = watchedEvent.getType();
            log.info("I'm LEADER!");

            log.info("[WATCH] CmdWatcher :: {} {}" + type.name(), watchedEvent.getPath());

            if (type.equals(Watcher.Event.EventType.NodeChildrenChanged)) {
                List<String> cmdNodeJsonTasks = curatorFramework.getChildren().forPath(zkTaskPath);
                List<Task> cmdTasks = new ArrayList<>();
                log.info("Found {} tasks: {}", cmdNodeJsonTasks.size(), cmdNodeJsonTasks);

                for (String cmdNodeJsonTask : cmdNodeJsonTasks) {
                    byte[] cmdJsonTask = curatorFramework.getData().forPath(zkTaskPath + "/" + cmdNodeJsonTask);
                    Map<String, Object> map = objectMapper.readValue(cmdJsonTask, Map.class);
                    cmdTasks.add(new CmdTask((String) map.get("cmd"), (Map<String, String>) map.get("files")));
                    curatorFramework.delete().forPath(zkTaskPath + "/" + cmdNodeJsonTask);
                }

                if (!cmdTasks.isEmpty()) {
                    zkTasksHandler.setTasks(cmdTasks);
                    zkTasksHandler.wakeup();
                }
            }
        } else {
            log.info("I'm not a leader ... only watch!");
        }
        curatorFramework.getChildren().usingWatcher(new CmdWatcher(zkTasksHandler, zkTaskPath)).forPath(ConfigFile.getInstance().getZkTaskPath());

    }
}
