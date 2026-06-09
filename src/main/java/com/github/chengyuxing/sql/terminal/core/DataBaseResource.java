package com.github.chengyuxing.sql.terminal.core;

import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.sql.BakiDao;
import com.github.chengyuxing.sql.XQLFileManager;
import com.github.chengyuxing.sql.terminal.common.Stdout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.chengyuxing.sql.terminal.common.Constants.APP_DIR;

public class DataBaseResource {
    private static final Logger log = LoggerFactory.getLogger(DataBaseResource.class);

    private final String dbName;
    private final BakiLoader bakiLoader;
    private final BakiDao baki;
    private final XQLFileManager xqlFileManager;

    public DataBaseResource(BakiLoader bakiLoader) {
        this.dbName = bakiLoader.dbName();
        this.bakiLoader = bakiLoader;
        this.baki = this.bakiLoader.getSysBaki();
        this.xqlFileManager = new XQLFileManager();
        init();
    }

    void init() {
        Path path = APP_DIR.resolve(Paths.get("completion", "database.xql"));
        if (Files.notExists(path)) {
            log.warn("Load {} failed: not found", path);
            return;
        }
        xqlFileManager.add("db", path.toUri().toString());
        xqlFileManager.init();
        baki.setXqlFileManager(xqlFileManager);
    }

    public List<String> getNames() {
        try (Stream<DataRow> s = baki.query("&" + XQLFileManager.encodeSqlReference("db", bakiLoader.dbName()))
                .args("username", bakiLoader.getUsername())
                .stream()) {
            return s.map(DataRow::<String>getFirstAs)
                    .collect(Collectors.toList());
        }
    }

    public Set<String> getSqlKeyWordsOrDefault() {
        Set<String> keywords = getSqlKeywords(dbName);
        if (keywords.isEmpty()) {
            keywords = getSqlKeywords("default");
        }
        return keywords;
    }

    public Set<String> getSqlKeywords(String dbName) {
        Path cnf = APP_DIR.resolve(Paths.get("completion", dbName + ".cnf"));
        if (!Files.exists(cnf)) {
            log.warn("Load {} failed: not found", cnf);
            Stdout.printlnWarning("Cannot load " + cnf + ": not found");
            return Collections.emptySet();
        }
        try (Stream<String> lines = Files.lines(cnf, StandardCharsets.UTF_8)) {
            return lines.map(line -> Arrays.asList(line.split("\\s+")))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());
        } catch (Exception e) {
            log.error("Load {}", cnf, e);
            return Collections.emptySet();
        }
    }
}
