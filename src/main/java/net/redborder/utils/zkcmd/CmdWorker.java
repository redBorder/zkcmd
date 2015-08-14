package net.redborder.utils.zkcmd;

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

    public CmdWorker(String cmd, Collection<String> filesToDelete, AtomicLong flag) {
        this.cmd = cmd;
        this.filesToDelete = filesToDelete;
        this.flag = flag;
    }

    public void run() {
        try {
            log.info("Executing: " + cmd);
            Process process = Runtime.getRuntime().exec(cmd);
            process.waitFor();

            if (process.exitValue() == 1) {
                log.error("Error executing: " + cmd);
                BufferedReader stdError = new BufferedReader(new
                        InputStreamReader(process.getErrorStream()));
                String s = null;
                while ((s = stdError.readLine()) != null) {
                    log.info(s);
                }
            }

            for (String filetoDelete : filesToDelete) {
                File file = new File(filetoDelete);
                file.delete();
            }
            log.info("Clean up!");
            flag.incrementAndGet();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
