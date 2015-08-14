package net.redborder.utils.zkcmd;

import net.redborder.utils.zkcmd.util.ConfigFile;
import net.redborder.utils.zkcmd.util.ZkUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;

public class ZkCmd {
    public static void main(String[] args) throws FileNotFoundException {
        final Logger log = LoggerFactory.getLogger(ZkCmd.class);
        ConfigFile.init();
        String zkWorkspace = "/rb_zkcmd";
        String zkTaskPath = ConfigFile.getInstance().getZkTaskPath();
        CuratorFramework curatorFramework = CuratorFrameworkFactory
                .newClient(ConfigFile.getInstance().getZkConnect(), new RetryNTimes(Integer.MAX_VALUE, 30000));
        curatorFramework.start();

        final ZkUtils zkUtils = new ZkUtils(curatorFramework, zkWorkspace);
        final CmdManager cmdManager = new CmdManager(zkUtils);

        log.info("Initiating ZkCmd ...");

        try {
            Thread.sleep(5000);

            if (zkUtils.isLeader()) {
                if (!curatorFramework.getState().equals(CuratorFrameworkState.STARTED)) {
                    log.info("Curator client isn't started .. waitting 5 sec.");
                    Thread.sleep(5000);
                }

                if (curatorFramework.checkExists().forPath(zkTaskPath) == null) {
                    log.info("ZkNode: {} doesn't exist, create it.", zkTaskPath);
                    curatorFramework.create().creatingParentsIfNeeded().forPath(zkTaskPath);
                }

            } else {
                Thread.sleep(1000);
            }

            zkUtils.registerNode();

        } catch (Exception e) {
            e.printStackTrace();
        }

        cmdManager.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Shutting down ...");
                cmdManager.shutdown();
                zkUtils.unRegisterNode();
                log.info("Shutdown!");
            }
        });
    }
}
