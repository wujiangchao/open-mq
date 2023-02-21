package com.open.openmq.common;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Properties;

/**
 * @Description TODO
 * @Date 2023/2/20 16:22
 * @Author jack wu
 */
public class MixAll {
    public static final String ROCKETMQ_HOME_ENV = "ROCKETMQ_HOME";
    public static final String ROCKETMQ_HOME_PROPERTY = "rocketmq.home.dir";


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
}
