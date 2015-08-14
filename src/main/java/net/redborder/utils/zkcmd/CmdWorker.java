package net.redborder.utils.zkcmd;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collection;

public class CmdWorker implements Runnable {
    final Logger log = LoggerFactory.getLogger(CmdWorker.class);

    String cmd;
    Collection<String> filesToDelete;

    public CmdWorker(String cmd, Collection<String> filesToDelete) {
        this.cmd = cmd;
        this.filesToDelete = filesToDelete;
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
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
