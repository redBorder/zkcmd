package net.redborder.utils.zkcmd;

import net.redborder.utils.zkcmd.util.Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

public class CmdWorker implements Runnable {
    final Logger log = LoggerFactory.getLogger(CmdWorker.class);

    String cmd;
    Collection<String> filesToDelete;
    AtomicLong flag;
    Integer id;
    Stats stats;

    public CmdWorker(Integer id, String cmd, Collection<String> filesToDelete, AtomicLong flag, Stats stats) {
        this.cmd = cmd;
        this.filesToDelete = filesToDelete;
        this.flag = flag;
        this.id = id;
        this.stats = stats;
    }

    public void run() {
        stats.incrementJob();
        try {
            log.info("ID[{}] Executing: {}", id, cmd);
            Process process = Runtime.getRuntime().exec(cmd);
            process.waitFor();

            if (process.exitValue() == 1) {
                printError(process);
            }

            for (String filetoDelete : filesToDelete) {
                File file = new File(filetoDelete);
                file.delete();
            }
            log.info("Clean up ID[{}]!", id);
            flag.incrementAndGet();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        stats.decrementJob();
    }

    private void printError(Process process){
        log.error("Error executing: " + cmd);
        BufferedReader stdError = new BufferedReader(new
                InputStreamReader(process.getErrorStream()));
        String s;
        try {
            while ((s = stdError.readLine()) != null) {
                log.error(s);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
