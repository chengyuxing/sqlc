package com.github.chengyuxing.sql.terminal.util;

import com.github.chengyuxing.common.io.FileResource;
import com.github.chengyuxing.sql.terminal.cli.App;
import com.github.chengyuxing.sql.terminal.common.Constants;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class PathUtils {
    public static boolean isFileURI(String path) {
        return new FileResource(path) {
            public boolean isURIPath() {
                return isURI();
            }
        }.isURIPath();
    }

    public static boolean isFilePath(String s) {
        String sep = File.separator;
        return s.trim().startsWith(sep)
                || s.startsWith("~" + sep)
                || s.startsWith("." + sep)
                || s.startsWith(".." + sep);
    }

    public static Path resolve(String path) {
        String myPath = path.trim();
        if ("~".equals(myPath)) {
            return Constants.USER_HOME;
        }
        if (myPath.startsWith("~" + File.separator)) {
            return Constants.USER_HOME.resolve(myPath.substring(2));
        }
        return Paths.get(myPath);
    }

    public static Path getAppDir() {
        try {
            URI appURI = App.class.getProtectionDomain()
                    .getCodeSource()
                    .getLocation()
                    .toURI();
            return Paths.get(appURI)
                    // ./lib
                    .getParent()
                    // ../sqlc-vx.x.x
                    //   - sqlc
                    //   - completion
                    //   - drivers
                    //   - lib
                    .getParent();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
