package com.github.chengyuxing.sql.terminal.core;

import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.terminal.common.Constants;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class BakiLoader implements AutoCloseable {
    private final HikariConfig config = new HikariConfig();
    private HikariDataSource userDataSource;
    private HikariDataSource sysDataSource;
    private BakiDao userBaki;
    private BakiDao sysBaki;
    private final String jdbcUrl;
    private String username = "";
    private String password = "";
    private String driver = "";

    BakiLoader(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public static BakiLoader of(String jdbcUrl) {
        return new BakiLoader(jdbcUrl);
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

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public void init() {
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(2);
        config.setJdbcUrl(jdbcUrl);
        if (!username.isEmpty()) {
            config.setUsername(username);
        }
        if (!password.isEmpty()) {
            config.setPassword(password);
        }
        if (!driver.isEmpty()) {
            config.setDriverClassName(driver);
        } else if (jdbcUrl.startsWith("jdbc:redis://")) {
            config.setDriverClassName("jdbc.RedisDriver");
        }
        userDataSource = new HikariDataSource(config);
        sysDataSource = new HikariDataSource(config);
        userBaki = new BakiDao(userDataSource);
        userBaki.setXqlFileManager(new XQLFileManager());
        sysBaki = new BakiDao(sysDataSource);
    }

    /**
     * 加载jdbc驱动包
     *
     * @param path 路径
     */
    public static void loadDrivers(String path) throws IOException {
        Path driverDir = Constants.APP_DIR.resolve(path);
        if (!Files.exists(driverDir)) {
            throw new FileNotFoundException("JDBC driver folder not exists: " + driverDir);
        }
        try (Stream<Path> s = Files.list(driverDir)) {
            s.filter(p -> p.toString().endsWith(".jar"))
                    .map(Path::toFile)
                    .forEach(Agent::addClassPath);
        }
    }

    public String dbName() {
        return userBaki.databaseInfo().getName();
    }

    public String dbVersion() {
        return userBaki.databaseInfo().getVersion();
    }

    public BakiDao getUserBaki() {
        return userBaki;
    }

    public BakiDao getSysBaki() {
        return sysBaki;
    }

    @Override
    public void close() {
        userDataSource.close();
        sysDataSource.close();
    }
}
