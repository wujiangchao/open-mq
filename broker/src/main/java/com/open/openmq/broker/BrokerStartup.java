package com.open.openmq.broker;

import sun.tools.jar.CommandLine;

import java.util.Properties;

/**
 * @Description TODO
 * @Date 2023/3/22 11:23
 * @Author jack wu
 */
public class BrokerStartup {
    public static Properties properties = null;
    public static CommandLine commandLine = null;
    public static String configFile = null;
    public static InternalLogger log;
    public static final SystemConfigFileHelper CONFIG_FILE_HELPER = new SystemConfigFileHelper();
}
