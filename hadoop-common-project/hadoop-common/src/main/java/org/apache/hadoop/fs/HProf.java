package org.apache.hadoop.fs;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;

public class HProf {

    // public final String pathHprofFile = "/home/hduser/local/";
    public final String pathHprofFile = "/home/rgmacedo/Dropbox/PhD/projects/";
    public String hprofFile;

    public Writer hprofWriter;
    
    private int messageCounter;
    public int messageFlush;
    private final int flusher = 10;

    public HProf() {
        this.hprofFile = this.generateLogFile();
        this.messageCounter = 0;
        this.messageFlush = 0;

        this.initHprofWriter();

        System.out.println(">> new HProf() created ...");
    }

    public String generateLogFile() {
        String logFile = null;
        
        try {
            logFile = 
                this.pathHprofFile +
                InetAddress.getLocalHost().getHostName() + "." + 
                System.currentTimeMillis() + ".log";
        
            } catch (Exception e) {
            System.out.println("HProf.generateLogFile >> UnknownHostException: " + e.getMessage());
            e.printStackTrace();
        }

        return logFile;
    }

    public int getMessageCounter () {
        return this.messageCounter;
    }

    public int getMessageFlush () {
        return this.messageFlush;
    }

    public void initHprofWriter () {
        System.out.println(">> HProf.initHprofWriter");
        try {
            if (this.hprofFile != null) {
                this.hprofWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this.hprofFile)));
            }
        } catch (Exception e) {
            System.out.println("HProf.initHprofWriter >> FileNotFoundException: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void writeLogMessage(String message) {
        try {
            this.hprofWriter.append(
                "[" + this.messageCounter + ":" +
                System.currentTimeMillis() + "] " +
                message + "\n");

            this.messageCounter++;
            this.messageFlush++;

            if (messageFlush == flusher) {
                this.hprofWriter.flush();
                this.messageFlush = 0;
            }

        } catch (Exception e) {
            System.out.println ("HProf.writeLogMessage >> IOException: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 
     * There is no need to flush the stream, since close() already does that.
     */
    public void closeHprofWriter () {
        System.out.println(">> HProf.closeHprofWriter");
        try {
            this.hprofWriter.close();
        } catch (Exception e) {
            System.out.println("HProf.closeHprofWriter >> IOException: " + e.getMessage());
            e.printStackTrace();
        }
    }


    public static void main (String[] args) throws InterruptedException {
        HProf hprof = new HProf();
        
        hprof.initHprofWriter();
        System.out.println(">> " + hprof.generateLogFile());
        
        for (int i = 0; i < 85; i++) {
            hprof.writeLogMessage(String.valueOf(i));
        }

        Thread.sleep(10000);

        hprof.closeHprofWriter();
    }

}