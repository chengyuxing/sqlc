package rabbit.sql.console.util;

import com.github.chengyuxing.common.utils.StringUtil;
import com.github.chengyuxing.sql.Baki;
import com.github.chengyuxing.sql.BakiDao;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class DataSourceLoader {
    private static final Logger log = LoggerFactory.getLogger(DataSourceLoader.class);

    private Baki baki;
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
     * @return baki
     * @throws SQLException sqlExp
     */
    public Baki getBaki() throws SQLException {
        if (baki == null) {
            dataSource = new HikariDataSource(config);
            baki = BakiDao.of(dataSource);
        }
        log.info("DataBase: {}", getDbInfo());
        return baki;
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
        DatabaseMetaData metaData = baki.metaData();
        return metaData.getDatabaseProductName() + " " + metaData.getDatabaseProductVersion();
    }

    public static Map<String, String> resolverArgs(String... args) {
        String[] argNames = new String[]{
                "-u",   //url
                "-p",   //password
                "-n",   //name
                "-f",   //format
                "-e",   //sql string or sql file
                "-skipHeader",
                "-d"    //multi sql block delimiter};
        };
        return Stream.of(args).filter(arg -> StringUtil.startsWiths(arg, argNames))
                .reduce(new HashMap<>(), (acc, curr) -> {
                    for (String name : argNames) {
                        if (curr.startsWith(name)) {
                            int prefixLength = name.length();
                            String key = curr.substring(0, prefixLength);
                            String value = curr.substring(key.length());
                            acc.put(key, value);
                            break;
                        }
                    }
                    return acc;
                }, (a, b) -> a);
    }
}
