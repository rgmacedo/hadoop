package org.apache.hadoop.fs;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;

import org.apache.commons.logging.Log;

public class HProf {

    // public final String pathHprofFile = "/home/rgmacedo/Dropbox/PhD/projects/hadoop/";
    
    public final String pathHprofFile = "/home/hduser/dfs/";
    public final String pathHprofLogging = "/home/hduser/dfs/hprof-logging.log";
    public String hprofFile;
    public String hprofLogging;
    
    public Writer hprofWriter;
    
    private int messageCounter;
    private int messageFlush;
    private int backgroundMessageCounter;
    private int dataMessageCounter;
    private int metadataMessageCounter;
    private final int flusher = 10;
    
    public static enum MessageType {
        BACK, DATA, META, HPROF
    };
    
    public Log LOG;
    
    /**
     * HProf class HProf is a Hadoop-based profile that profiles user-defined
     * messages.
     */
    public HProf(Log log) {
        LOG = log;

        this.hprofFile = this.generateLogFile();

        this.messageCounter = 0;
        this.messageFlush = 0;
        this.backgroundMessageCounter = 0;
        this.dataMessageCounter = 0;
        this.metadataMessageCounter = 0;

        this.initHprofWriter();


        LOG.info(">> new HProf() created ...");
    }

    public String generateLogFile() {
        String logFile = null;

        try {
            logFile = this.pathHprofFile + InetAddress.getLocalHost().getHostName() + "." + System.currentTimeMillis()
                    + ".log";

        } catch (Exception e) {
            LOG.info("HProf.generateLogFile >> UnknownHostException: " + e.getMessage());
            e.printStackTrace();
        }

        return logFile;
    }

    public int getMessageCounter() {
        return this.messageCounter;
    }

    public int getMessageFlush() {
        return this.messageFlush;
    }

    public void initHprofWriter() {
        LOG.info(">> HProf.initHprofWriter");
        try {
            if (this.hprofFile != null) {
                this.hprofWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this.hprofFile)));
            }
        } catch (Exception e) {
            LOG.info("HProf.initHprofWriter >> FileNotFoundException: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void writeLogMessage(MessageType type, String method, String message) {
        try {
            this.hprofWriter.append(System.currentTimeMillis() + " " + type.toString() + " " + method + ": " + message + "\n");

            this.messageCounter++;
            this.messageFlush++;

            if (messageFlush == flusher) {
                this.hprofWriter.flush();
                this.messageFlush = 0;
            }

            switch (type) {
            case BACK:
                this.backgroundMessageCounter++;
                break;
            case DATA:
                this.dataMessageCounter++;
                break;
            case META:
                this.metadataMessageCounter++;
            default:
                break;
            }

        } catch (Exception e) {
            LOG.info("HProf.writeLogMessage >> IOException: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 
     * There is no need to flush the stream, since close() already does that.
     */
    public void closeHprofWriter() {
        LOG.info(">> HProf.closeHprofWriter");
        try {
            this.hprofWriter.close();
        } catch (Exception e) {
            LOG.info("HProf.closeHprofWriter >> IOException: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // public static void main (String[] args) throws InterruptedException {
    // HProf hprof = new HProf();

    // hprof.initHprofWriter();
    // System.out.println(">> " + hprof.generateLogFile());

    // for (int i = 0; i < 85; i++) {
    // hprof.writeLogMessage(MessageType.META, "operation-"+i, String.valueOf(i));
    // }

    // Thread.sleep(10000);

    // hprof.closeHprofWriter();
    // }

}