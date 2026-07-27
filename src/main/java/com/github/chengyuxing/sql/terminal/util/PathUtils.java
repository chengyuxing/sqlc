package com.github.chengyuxing.sql.terminal.util;

import com.github.chengyuxing.sql.terminal.cli.App;
import com.github.chengyuxing.sql.terminal.common.Constants;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.util.Comparator;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class PathUtils {
    public static boolean isFileURI(String path) {
        return URI.create(path).isAbsolute();
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

    public static Path createTmpFile(Path source) {
        return Constants.SQLC_TEMP_PATH.resolve(source.getFileName() + "." + System.currentTimeMillis() + ".tmp");
    }

    public static void usingTmpFile(Path source, Consumer<Path> consumeTempFile) throws IOException {
        Path tmp = createTmpFile(source);
        try {
            consumeTempFile.accept(tmp);
            Files.move(tmp, source, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    public static boolean isDirectoryEmpty(Path dir) throws IOException {
        if (!Files.isDirectory(dir)) {
            return true;
        }
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
            return !ds.iterator().hasNext();
        }
    }

    public static void deleteFileRecursive(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (Stream<Path> s = Files.walk(path)) {
            s.sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }
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
                    //  |- sqlc
                    //  |- completion
                    //  |- drivers
                    //  |- lib
                    .getParent();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
