package rabbit.sql.console.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.chengyuxing.common.DataRow;
import com.github.chengyuxing.common.utils.StringUtil;

import java.util.Arrays;

public class ObjectUtil {
    public final static ObjectMapper JSON = new ObjectMapper();

    public static String getJson(DataRow row) throws JsonProcessingException {
        DataRow res = jsonSerializedExceptBlob(row);
        return JSON.writerWithDefaultPrettyPrinter().writeValueAsString(res.toMap());
    }

    public static DataRow jsonSerializedExceptBlob(DataRow row) {
        DataRow res = row.cloneNew();
        for (String name : row.getNames()) {
            Object v = row.get(name);
            if (v instanceof byte[]) {
                res = res.put(name, wrapObjectForSerialized(v));
            }
        }
        return res;
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
