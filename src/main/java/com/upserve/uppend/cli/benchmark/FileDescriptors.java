package com.upserve.uppend.cli.benchmark;

import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;
import java.lang.management.*;
import java.lang.reflect.Method;
import java.util.TimerTask;

// Based on: https://gist.github.com/jtai/5408684
public class FileDescriptors {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static FileDescriptors fileDescriptors = new FileDescriptors();

    private final OperatingSystemMXBean osMxBean;

    private FileDescriptors(){
        osMxBean = ManagementFactory.getOperatingSystemMXBean();
    }

    public static long getMax() {
        try {
            Method getMaxFileDescriptorCountField = fileDescriptors.osMxBean.getClass().getDeclaredMethod("getMaxFileDescriptorCount");
            getMaxFileDescriptorCountField.setAccessible(true);

            return (long) getMaxFileDescriptorCountField.invoke(fileDescriptors.osMxBean);
        } catch (Exception e) {
            log.error("Unable to getValue max file descriptors from OperatingSystemMXBean", e);
            return 0;
        }
    }

    public static long getOpen() {
        try {
            Method getOpenFileDescriptorCountField = fileDescriptors.osMxBean.getClass().getDeclaredMethod("getOpenFileDescriptorCount");
            getOpenFileDescriptorCountField.setAccessible(true);
            return (long) getOpenFileDescriptorCountField.invoke(fileDescriptors.osMxBean);
        } catch (Exception e) {
            log.error("Unable to getValue max file descriptors from OperatingSystemMXBean", e);
            return 0;
        }
    }

    public static TimerTask fileDescriptorWatcher() {
        return  new TimerTask() {
            @Override
            public void run() {
                log.info("File Descriptors: {} open / {} max", getOpen(), getMax());
            }
        };
    }
}
