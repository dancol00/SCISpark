package it.unisa.di.dif.utils;

/**
 * It's a singleton class that contains all the constants used in the application
 */
public class Constant {
    public static final String VALUE_SEPARATOR_FOR_NOISE_FILE = "\t";
    public static final Character LINE_START_FOR_INFO_IN_NOISE_FILE = '*';
    public static final Character LINE_START_FOR_CHANNEL_IN_NOISE_FILE = '|';
    public static final String RED_CHANNEL_NAME = "red";
    public static final String GREEN_CHANNEL_NAME = "green";
    public static final String BLUE_CHANNEL_NAME = "blue";

    private static Constant instance;

    private Constant() {}

    public static Constant getInstance() {
        if(instance == null) {
            instance = new Constant();
        }
        return instance;
    }

    public String getAppdir() {
        return ".";
    }

    public boolean isWriteMessageLogOnConsole() {
        return true;
    }

    public String getStringLogName() {
        return "SCILib";
    }

    public boolean isWriteMessageOnStderr() {
        return true;
    }
}
