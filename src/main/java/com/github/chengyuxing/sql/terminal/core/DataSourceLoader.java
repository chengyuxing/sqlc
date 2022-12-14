package com.github.chengyuxing.sql.terminal.core;

import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.terminal.vars.Constants;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.stream.Stream;

public class DataSourceLoader {
    private final HikariConfig config = new HikariConfig();
    private HikariDataSource dataSource;
    private HikariDataSource sysDataSource;
    private UserBaki baki;
    private BakiDao sysBaki;
    private final String jdbcUrl;
    private String username = "";
    private String password = "";
    private String dbName = "";

    DataSourceLoader(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
        this.config.setJdbcUrl(jdbcUrl);
    }

    public static DataSourceLoader of(String jdbcUrl) {
        return new DataSourceLoader(jdbcUrl);
    }

    public String getUsername() {
        return username;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDbName() {
        return dbName;
    }

    public void init() throws SQLException {
        config.setMaximumPoolSize(2);
        config.setUsername(username);
        config.setPassword(password);

        dataSource = new HikariDataSource(config);
        sysDataSource = new HikariDataSource(config);

        try (Connection connection = sysDataSource.getConnection()) {
            dbName = connection.getMetaData().getDatabaseProductName().toLowerCase();
        }
    }

    /**
     * 加载jdbc驱动包
     *
     * @param path 路径
     * @throws NoSuchMethodException exp
     */
    public static void loadDrivers(String path) throws NoSuchMethodException, FileNotFoundException {
        File driverDir = Constants.APP_DIR.getParent().resolve(path).toFile();
        if (!driverDir.exists()) {
            throw new FileNotFoundException("jdbc driver folder not exists: " + driverDir);
        }
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
    public UserBaki getUserBaki() {
        if (baki == null) {
            baki = new UserBaki(dataSource, username);
            if (Constants.IS_XTERM) {
                baki.setHighlightSql(true);
            }
            baki.setCheckParameterType(false);
            baki.setStrictDynamicSqlArg(false);
        }
        return baki;
    }

    public BakiDao getSysBaki() {
        if (sysBaki == null) {
            sysBaki = new BakiDao(sysDataSource);
            sysBaki.setHighlightSql(false);
            sysBaki.setCheckParameterType(false);
            sysBaki.setStrictDynamicSqlArg(false);
        }
        return sysBaki;
    }

    public void release() {
        dataSource.close();
        sysDataSource.close();
    }
}
