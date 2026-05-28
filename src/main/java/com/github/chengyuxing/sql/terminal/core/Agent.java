package com.github.chengyuxing.sql.terminal.core;

import java.io.File;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ServiceLoader;
import java.util.jar.JarFile;

public class Agent {
    private static Instrumentation inst = null;

    public static void agentmain(final String a, final Instrumentation inst) {
        Agent.inst = inst;
    }

    public static void addClassPath(File jar) {
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        try {
            // jdk9+
            if (!(classLoader instanceof URLClassLoader)) {
                inst.appendToSystemClassLoaderSearch(new JarFile(jar));
                return;
            }
            //jdk8
            Method m = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
            m.setAccessible(true);
            m.invoke(classLoader, jar.toURI().toURL());
            ServiceLoader<Driver> drivers = ServiceLoader.load(Driver.class, classLoader);
            for (Driver driver : drivers) {
                DriverManager.registerDriver(driver);
            }
        } catch (IOException | NoSuchMethodException | IllegalAccessException | InvocationTargetException |
                 SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
