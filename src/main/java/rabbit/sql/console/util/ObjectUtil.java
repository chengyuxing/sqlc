package rabbit.sql.console.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.chengyuxing.common.DataRow;

import java.util.Arrays;
import java.util.Formatter;

public class ObjectUtil {
    private final static ObjectMapper JSON = new ObjectMapper();

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
            String strSize = "0KB";
            final Formatter fmt = new Formatter();
            if (bytesArr.length > 1048576) {
                strSize = fmt.format("%.2f", bytesArr.length / 1048576.0) + "MB";
            } else if (bytesArr.length > 0) {
                strSize = fmt.format("%.2f", bytesArr.length / 1024.0) + "KB";
            }
            return "blob:" + strSize;
        }
        return obj;
    }
}
