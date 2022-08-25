package Common;

import java.io.*;

public class Logger {
    private File logFile = new File("logs/log.txt");
    private FileWriter writer = null;

    public Logger() {
        try {
            writer = new FileWriter(logFile);
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    public void log(String msg){
        try {
            writer.write("\t"+msg+"\n");
            writer.flush();
        }
        catch (IOException | NullPointerException e){
            e.printStackTrace();
        }
    }
}
