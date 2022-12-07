package com.github.chengyuxing.sql.terminal.core;

import com.github.chengyuxing.sql.terminal.vars.Constants;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Optional;
import java.util.stream.Stream;

public class DataSourceLoader {
    private final HikariConfig config = new HikariConfig();
    private HikariDataSource dataSource;
    private SingleBaki baki;
    private String username = "";
    private String password = "";

    DataSourceLoader(String jdbcUrl) {
        this.config.setJdbcUrl(jdbcUrl);
    }

    public static DataSourceLoader of(String jdbcUrl) {
        return new DataSourceLoader(jdbcUrl);
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void init() {
        config.setMaximumPoolSize(5);
        config.setUsername(username);
        config.setPassword(password);
        dataSource = new HikariDataSource(config);
    }

    /**
     * 加载jdbc驱动包
     *
     * @param path 路径
     * @throws NoSuchMethodException exp
     */
    public static void loadDrivers(String path) throws NoSuchMethodException {
        File driverDir = new File(Constants.APP_DIR + File.separator + path);
        URLClassLoader loader = (URLClassLoader) ClassLoader.getSystemClassLoader();
        Method add = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
        add.setAccessible(true);

        Optional.ofNullable(driverDir.listFiles())
                .ifPresent(files -> Stream.of(files)
                        .filter(f -> f.getName().endsWith(".jar"))
                        .forEach(j -> {
                            try {
                                add.invoke(loader, j.toURI().toURL());
                            } catch (IllegalAccessException | InvocationTargetException | MalformedURLException e) {
                                e.printStackTrace();
                            }
                        }));

    }

    /**
     * 获取baki实例
     *
     * @return baki
     */
    public SingleBaki getBaki() {
        if (baki == null) {
            baki = new SingleBaki(dataSource, username);
            if (Constants.IS_XTERM) {
                baki.setHighlightSql(true);
            }
            baki.setCheckParameterType(false);
        }
        return baki;
    }

    public void release() {
        dataSource.close();
    }
}
