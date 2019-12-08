package org.arpit.spark.common.util;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class LoggerUtil {

    public static void disableSparkLogs() {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
    }
}
