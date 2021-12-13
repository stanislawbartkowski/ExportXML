package util;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Log {

    private final static Logger LOGGER = Logger.getLogger(Log.class.getName());

    public static void severe(String mess, Throwable e, boolean exit) {
        if (e != null) LOGGER.log(Level.SEVERE, mess, e);
        else LOGGER.log(Level.SEVERE, mess);
        if (exit) System.exit(4);
    }

    public static void severe(String mess, Throwable e) {
        severe(mess, e, true);
    }


    public static void severe(String mess) {
        severe(mess, null);
    }

    public static void info(String mess) {
        LOGGER.info(mess);
    }
}