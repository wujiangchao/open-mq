package com.open.openmq.logging;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author 34246
 */
public abstract class InternalLoggerFactory {

    public static final String LOGGER_SLF4J = "slf4j";

    public static final String LOGGER_INNER = "inner";

    public static final String DEFAULT_LOGGER = LOGGER_SLF4J;

    public static final String BROKER_CONTAINER_NAME = "BrokerContainer";

    /**
     * Loggers with following name will be directed to default logger for LogTail parser.
     */
    public static final String CONSUMER_STATS_LOGGER_NAME = "OpenmqConsumerStats";
    public static final String COMMERCIAL_LOGGER_NAME = "OpenmqCommercial";
    public static final String ACCOUNT_LOGGER_NAME = "OpenmqAccount";

    private static String loggerType = null;

    public static final ThreadLocal<String> BROKER_IDENTITY = new ThreadLocal<String>();

    private static ConcurrentHashMap<String, InternalLoggerFactory> loggerFactoryCache = new ConcurrentHashMap<String, InternalLoggerFactory>();

    public static InternalLogger getLogger(Class clazz) {
        return getLogger(clazz.getName());
    }

    public static InternalLogger getLogger(String name) {
        return getLoggerFactory().getLoggerInstance(name);
    }

    private static InternalLoggerFactory getLoggerFactory() {
        InternalLoggerFactory internalLoggerFactory = null;
        if (loggerType != null) {
            internalLoggerFactory = loggerFactoryCache.get(loggerType);
        }
        if (internalLoggerFactory == null) {
            internalLoggerFactory = loggerFactoryCache.get(DEFAULT_LOGGER);
        }
        if (internalLoggerFactory == null) {
            internalLoggerFactory = loggerFactoryCache.get(LOGGER_INNER);
        }
        if (internalLoggerFactory == null) {
            throw new RuntimeException("[OpenMQ] Logger init failed, please check logger");
        }
        return internalLoggerFactory;
    }

    public static void setCurrentLoggerType(String type) {
        loggerType = type;
    }

    static {
        try {
            new Slf4jLoggerFactory();
        } catch (Throwable e) {
            //ignore
        }
        try {
            new InnerLoggerFactory();
        } catch (Throwable e) {
            //ignore
        }
    }

    protected void doRegister() {
        String loggerType = getLoggerType();
        if (loggerFactoryCache.get(loggerType) != null) {
            return;
        }
        loggerFactoryCache.put(loggerType, this);
    }

    protected abstract void shutdown();

    protected abstract InternalLogger getLoggerInstance(String name);

    protected abstract String getLoggerType();
}
