package rabbit.sql.console.core;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rabbit.sql.Light;
import rabbit.sql.dao.LightDao;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataSourceLoader {
    private static final Logger log = LoggerFactory.getLogger(DataSourceLoader.class);

    private Light light;
    private final HikariConfig config;
    private HikariDataSource dataSource;

    DataSourceLoader(HikariConfig config) {
        this.config = config;
    }

    public static DataSourceLoader of(String jdbcUrl, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setMaximumPoolSize(2);
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setDriverClassName(getDriverClassName(jdbcUrl));
        return new DataSourceLoader(config);
    }

    /**
     * 加载jdbc驱动包
     *
     * @param path 路径
     * @throws NoSuchMethodException exp
     */
    public static void loadDrivers(String path) throws NoSuchMethodException {
        String classPath = System.getProperty("java.class.path");
        String absolutePath = classPath.substring(0, classPath.lastIndexOf(File.separator));
        File driverDir = new File(absolutePath + File.separator + path);
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
     * 获取light实例
     *
     * @return light
     * @throws SQLException sqlExp
     */
    public Light getLight() throws SQLException {
        if (light == null) {
            dataSource = new HikariDataSource(config);
            light = LightDao.of(dataSource);
        }
        log.info("Login: {}", getDbInfo());
        return light;
    }

    public void release() {
        dataSource.close();
    }

    /**
     * 获取数据库信息
     *
     * @return 数据库信息
     * @throws SQLException sqlExp
     */
    public String getDbInfo() throws SQLException {
        DatabaseMetaData metaData = light.getMetaData();
        return metaData.getDatabaseProductName() + " " + metaData.getDatabaseProductVersion();
    }

    /**
     * 根据jdbcUrl获取驱动名称
     *
     * @param url jdbcUrl
     * @return 驱动名
     */
    public static String getDriverClassName(String url) {
        if (url.startsWith("jdbc:oracle")) {
            return "oracle.jdbc.driver.OracleDriver";
        }
        if (url.startsWith("jdbc:postgresql")) {
            return "org.postgresql.Driver";
        }
        if (url.startsWith("jdbc:sqlserver")) {
            return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        }
        if (url.startsWith("jdbc:mysql")) {
            return "com.mysql.jdbc.Driver";
        }
        if (url.startsWith("jdbc:sqlite")) {
            return "org.sqlite.JDBC";
        }
        throw new IllegalArgumentException("jdbc url error:" + url);
    }

    public static Map<String, String> resolverArgs(String... args) {
        List<String> argNames = Arrays.asList(
                "-u",   //url
                "-p",   //password
                "-n",   //name
                "-f",   //format
                "-s",   //savePath
                "-e"    //executed sql
        );
        return Stream.of(args)
                .filter(arg -> arg.length() >= 2)
                .filter(arg -> argNames.contains(arg.substring(0, 2)))
                .collect(Collectors.toMap(k -> k.substring(0, 2), v -> v.substring(2)));
    }
}
