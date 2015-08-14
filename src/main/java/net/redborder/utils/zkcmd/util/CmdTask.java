package net.redborder.utils.zkcmd.util;

import net.redborder.clusterizer.MappedTask;

import java.util.Map;

public class CmdTask extends MappedTask {

    public CmdTask(){
    }

    public CmdTask(Map<String, Object> data){
        initialize(data);
    }

    public CmdTask(String cmd){
        setCmd(cmd);
    }

    public CmdTask(String cmd, Map<String, String> files){
        setCmd(cmd);
        setFiles(files);
    }

    public void setCmd(String cmd){
        setData("cmd", cmd);
    }

    public void setFiles(Map<String, String> args){
        setData("files", args);
    }

    public String getCmd(){
        return getData("cmd");
    }

    public Map<String, String> getFiles(){
        return getData("files");
    }
}
