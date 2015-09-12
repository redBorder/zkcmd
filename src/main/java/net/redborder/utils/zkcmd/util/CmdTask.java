package net.redborder.utils.zkcmd.util;

import java.util.HashMap;
import java.util.Map;

public class CmdTask {

    private Map<String, Object> task = new HashMap<>();

    public CmdTask(Map<String, Object> data){
        task.putAll(data);
    }

    public CmdTask(String cmd){
        setCmd(cmd);
    }

    public CmdTask(String cmd, Map<String, String> files){
        setCmd(cmd);
        setFiles(files);
    }

    public void setCmd(String cmd){
        task.put("cmd", cmd);
    }

    public void setFiles(Map<String, String> args){
        task.put("files", args);
    }

    public String getCmd(){
        return (String) task.get("cmd");
    }

    public Map<String, String> getFiles(){
        return (Map<String, String>) task.get("files");
    }
}
