package com.open.openmq.common;

import com.open.openmq.common.annotation.ImportantField;
import com.open.openmq.common.constant.LoggerName;
import com.open.openmq.common.utils.IOTinyUtils;
import com.open.openmq.logging.InternalLogger;
import com.open.openmq.logging.InternalLoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

/**
 * @Description TODO
 * @Date 2023/2/20 16:22
 * @Author jack wu
 */
public class MixAll {

    public static final String ROCKETMQ_HOME_ENV = "OPENMQ_HOME";
    public static final String ROCKETMQ_HOME_PROPERTY = "openmq.home.dir";
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);
    public static final long MASTER_ID = 0L;
    public static final String METADATA_SCOPE_GLOBAL = "__global__";
    public static final String NAMESRV_ADDR_ENV = "NAMESRV_ADDR";
    public static final String NAMESRV_ADDR_PROPERTY = "rocketmq.namesrv.addr";
    public static final String MULTI_PATH_SPLITTER = System.getProperty("rocketmq.broker.multiPathSplitter", ",");
    public static final String RETRY_GROUP_TOPIC_PREFIX = "%RETRY%";
    public static final String DLQ_GROUP_TOPIC_PREFIX = "%DLQ%";
    public static final String CID_RMQ_SYS_PREFIX = "CID_RMQ_SYS_";

    public static void properties2Object(final Properties p, final Object object) {
        Method[] methods = object.getClass().getMethods();
        for (Method method : methods) {
            String mn = method.getName();
            if (mn.startsWith("set")) {
                try {
                    String tmp = mn.substring(4);
                    String first = mn.substring(3, 4);

                    String key = first.toLowerCase() + tmp;
                    String property = p.getProperty(key);
                    if (property != null) {
                        Class<?>[] pt = method.getParameterTypes();
                        if (pt != null && pt.length > 0) {
                            String cn = pt[0].getSimpleName();
                            Object arg = null;
                            if (cn.equals("int") || cn.equals("Integer")) {
                                arg = Integer.parseInt(property);
                            } else if (cn.equals("long") || cn.equals("Long")) {
                                arg = Long.parseLong(property);
                            } else if (cn.equals("double") || cn.equals("Double")) {
                                arg = Double.parseDouble(property);
                            } else if (cn.equals("boolean") || cn.equals("Boolean")) {
                                arg = Boolean.parseBoolean(property);
                            } else if (cn.equals("float") || cn.equals("Float")) {
                                arg = Float.parseFloat(property);
                            } else if (cn.equals("String")) {
                                arg = property;
                            } else {
                                continue;
                            }
                            method.invoke(object, arg);
                        }
                    }
                } catch (Throwable ignored) {
                }
            }
        }
    }

    public static void printObjectProperties(final InternalLogger logger, final Object object) {
        printObjectProperties(logger, object, false);
    }

    public static void printObjectProperties(final InternalLogger logger, final Object object,
                                             final boolean onlyImportantField) {
        Field[] fields = object.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (!Modifier.isStatic(field.getModifiers())) {
                String name = field.getName();
                if (!name.startsWith("this")) {
                    if (onlyImportantField) {
                        Annotation annotation = field.getAnnotation(ImportantField.class);
                        if (null == annotation) {
                            continue;
                        }
                    }

                    Object value = null;
                    try {
                        field.setAccessible(true);
                        value = field.get(object);
                        if (null == value) {
                            value = "";
                        }
                    } catch (IllegalAccessException e) {
                        log.error("Failed to obtain object properties", e);
                    }

                    if (logger != null) {
                        logger.info(name + "=" + value);
                    } else {
                    }
                }
            }
        }
    }

    public static void string2File(final String str, final String fileName) throws IOException {

        String tmpFile = fileName + ".tmp";
        string2FileNotSafe(str, tmpFile);

        String bakFile = fileName + ".bak";
        String prevContent = file2String(fileName);
        if (prevContent != null) {
            string2FileNotSafe(prevContent, bakFile);
        }

        File file = new File(fileName);
        file.delete();

        file = new File(tmpFile);
        file.renameTo(new File(fileName));
    }

    public static void string2FileNotSafe(final String str, final String fileName) throws IOException {
        File file = new File(fileName);
        File fileParent = file.getParentFile();
        if (fileParent != null) {
            fileParent.mkdirs();
        }
        IOTinyUtils.writeStringToFile(file, str, "UTF-8");
    }

    public static String file2String(final String fileName) throws IOException {
        File file = new File(fileName);
        return file2String(file);
    }

    public static String file2String(final File file) throws IOException {
        if (file.exists()) {
            byte[] data = new byte[(int) file.length()];
            boolean result;

            FileInputStream inputStream = null;
            try {
                inputStream = new FileInputStream(file);
                int len = inputStream.read(data);
                result = len == data.length;
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
            }

            if (result) {
                return new String(data, "UTF-8");
            }
        }
        return null;
    }
    public static Properties object2Properties(final Object object) {
        Properties properties = new Properties();

        Field[] fields = object.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (!Modifier.isStatic(field.getModifiers())) {
                String name = field.getName();
                if (!name.startsWith("this")) {
                    Object value = null;
                    try {
                        field.setAccessible(true);
                        value = field.get(object);
                    } catch (IllegalAccessException e) {
                        log.error("Failed to handle properties", e);
                    }

                    if (value != null) {
                        properties.setProperty(name, value.toString());
                    }
                }
            }
        }

        return properties;
    }

    public static String properties2String(final Properties properties) {
        return properties2String(properties, false);
    }

    public static String properties2String(final Properties properties, final boolean isSort) {
        StringBuilder sb = new StringBuilder();
        Set<Map.Entry<Object, Object>> entrySet = isSort ? new TreeMap<>(properties).entrySet() : properties.entrySet();
        for (Map.Entry<Object, Object> entry : entrySet) {
            if (entry.getValue() != null) {
                sb.append(entry.getKey().toString() + "=" + entry.getValue().toString() + "\n");
            }
        }
        return sb.toString();
    }

    public static boolean isSysConsumerGroup(final String consumerGroup) {
        return consumerGroup.startsWith(CID_RMQ_SYS_PREFIX);
    }
}
