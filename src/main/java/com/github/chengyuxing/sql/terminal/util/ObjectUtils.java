package com.github.chengyuxing.sql.terminal.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.io.FileResource;

import java.util.Arrays;
import java.util.Map;

public class ObjectUtils {
    public final static ObjectMapper JSON = new ObjectMapper();
    public final static ObjectWriter PRETTY_JSON = JSON.writerWithDefaultPrettyPrinter();

    public static String getJson(DataRow row) throws JsonProcessingException {
        for (Map.Entry<String, Object> e : row.entrySet()) {
            if (e.getValue() instanceof byte[]) {
                e.setValue(wrapObjectForSerialized(e.getValue()));
            }
        }
        return PRETTY_JSON.writeValueAsString(row);
    }

    public static Object wrapObjectForSerialized(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof byte[]) {
            return "blob:" + FileResource.formatFileSize(((byte[]) obj).length);
        }
        if (obj.getClass().isArray()) {
            return Arrays.toString((Object[]) obj);
        }
        return obj;
    }
}
