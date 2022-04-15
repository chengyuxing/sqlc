package rabbit.sql.console.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.utils.StringUtil;

import java.util.Arrays;

public class ObjectUtil {
    public final static ObjectMapper JSON = new ObjectMapper();

    public static String getJson(DataRow row) throws JsonProcessingException {
        for (String name : row.keySet()) {
            Object v = row.get(name);
            if (v instanceof byte[]) {
                row.put(name, wrapObjectForSerialized(v));
            }
        }
        return JSON.writerWithDefaultPrettyPrinter().writeValueAsString(row);
    }

    public static Object wrapObjectForSerialized(Object obj) {
        if (obj == null) {
            return null;
        }
        if (Object[].class.isAssignableFrom(obj.getClass())) {
            return Arrays.toString((Object[]) obj);
        }
        if (obj instanceof byte[]) {
            byte[] bytesArr = (byte[]) obj;
            return "blob:" + StringUtil.getSize(bytesArr);
        }
        return obj;
    }
}
