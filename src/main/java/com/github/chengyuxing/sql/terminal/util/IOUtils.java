package com.github.chengyuxing.sql.terminal.util;

import java.io.*;
import java.nio.charset.Charset;
import java.util.StringJoiner;

public class IOUtils {
    public static String toString(InputStream in, Charset charset) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, charset))) {
            StringJoiner sb = new StringJoiner("\n");
            String line;
            while ((line = reader.readLine()) != null) {
                sb.add(line);
            }
            return sb.toString();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static boolean isPipedInput() throws IOException {
        return System.console() == null && System.in.available() > 0;
    }
}
