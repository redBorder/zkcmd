package net.redborder.utils.zkcmd;

import net.redborder.clusterizer.Task;
import net.redborder.clusterizer.ZkTasksHandler;
import net.redborder.utils.zkcmd.util.CmdTask;
import net.redborder.utils.zkcmd.util.ConfigFile;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ZkCmd {
    public static void main(String[] args) throws FileNotFoundException {
        final Logger log = LoggerFactory.getLogger(ZkCmd.class);
        log.info("Initiate ZkCmd!!");
        ConfigFile.init();
        String zkWork = "/rb_zkcmd";
        String zkTaskPath = ConfigFile.getInstance().getZkTaskPath();
        ObjectMapper objectMapper = new ObjectMapper();
        final ZkTasksHandler zkTasksHandler = new ZkTasksHandler(ConfigFile.getInstance().getZkConnect(), zkWork);
        final CmdManager cmdManager = new CmdManager(zkTasksHandler.getCuratorClient());
        zkTasksHandler.addListener(cmdManager);
        CuratorFramework curatorFramework = zkTasksHandler.getCuratorClient();
        InterProcessSemaphoreMutex mutex = new InterProcessSemaphoreMutex(curatorFramework, zkWork + "/watched_mutex");

        cmdManager.start();

        try {
            if (!curatorFramework.getState().equals(CuratorFrameworkState.STARTED)) {
                log.info("Curator client isn't started .. waitting 5 sec.");
                Thread.sleep(5000);
            }


            if (curatorFramework.checkExists().forPath(zkTaskPath) == null) {
                log.info("ZkNode: {} doesn't exist, create it.", zkTaskPath);
                curatorFramework.create().forPath(zkTaskPath);
            } else {
                List<String> cmdNodeJsonTasks = curatorFramework.getChildren().forPath(zkTaskPath);
                List<Task> cmdTasks = new ArrayList<>();
                log.info("Found {} tasks: {}", cmdNodeJsonTasks.size(), cmdNodeJsonTasks);

                for (String cmdNodeJsonTask : cmdNodeJsonTasks) {
                    byte[] cmdJsonTask = curatorFramework.getData().forPath(zkTaskPath + "/" + cmdNodeJsonTask);
                    Map<String, Object> map = objectMapper.readValue(cmdJsonTask, Map.class);
                    cmdTasks.add(new CmdTask((String) map.get("cmd"), (Map<String, String>) map.get("files")));
                    curatorFramework.delete().forPath(zkTaskPath + "/" + cmdNodeJsonTask);
                }

                zkTasksHandler.setTasks(cmdTasks);
                zkTasksHandler.wakeup();
            }

            curatorFramework.getChildren().usingWatcher(new CmdWatcher(zkTasksHandler, zkTaskPath, mutex)).forPath(zkTaskPath);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Shutting down ...");
                cmdManager.shutdown();
                zkTasksHandler.end();
                log.info("Shutdown!");
            }
        });
    }
}
