package rabbit.sql.connsole.test;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class ReadBigFileTests {
    // 899Mb
    static final String path = "/Users/chengyuxing/Downloads/big.json";
    static final ObjectMapper json = new ObjectMapper();

    @Test
    public void testJackson() throws Exception {
        try (MappingIterator<Map<String, Object>> iterator = json.reader().forType(Map.class).readValues(new File(path))) {
            Map<String, Object> obj = null;
            while (iterator.hasNext()) {
                obj = iterator.next();
            }
            System.out.println(obj);
        }
    }

    @Test
    public void testTsv() throws Exception{
        try (Stream<String> s = Files.lines(Paths.get("/Users/chengyuxing/Downloads/big.tsv"))){
            AtomicReference<String> stringAtomicReference = new AtomicReference<>();
            s.forEach(stringAtomicReference::set);
            System.out.println(stringAtomicReference);
        }
    }

    @Test
    public void testCustom() throws Exception {
        readJson(path);
    }

    public void readJson(String path) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(Files.newInputStream(Paths.get(path)))))) {
            String line;
            String prevLine = "";
            Map<String, Object> map = null;
            out:
            while ((line = reader.readLine()) != null) {
                int start;
                int end = -1;
                if (!prevLine.equals("")) {
                    start = prevLine.indexOf("{");
                } else {
                    start = line.indexOf("{");
                    end = line.lastIndexOf("}");
                }
                if (start != -1) {
                    StringJoiner jsonObj = new StringJoiner("\n");
                    if (!prevLine.equals("")) {
                        jsonObj.add(prevLine.substring(start));
                        prevLine = "";
                        jsonObj.add(line);
                    } else {
                        String l = line.substring(start);
                        if (end != -1) {
                            l = l.substring(0, l.lastIndexOf("}") + 1);
                        }
                        jsonObj.add(l);
                    }
                    if (end != -1 && start < end) {
                        map = json.readValue(jsonObj.toString(), Map.class);
                        continue;
                    }
                    int count = 1;
                    while (true) {
                        String next = reader.readLine();
                        if (next != null) {
                            int nextStartIdx = next.indexOf("{");
                            int nextEndIdx = next.indexOf("}");
                            if (nextEndIdx != -1) {
                                count--;
                                if (count == 0) {
                                    jsonObj.add(next.substring(0, nextEndIdx + 1));
                                    // },{
                                    if (nextStartIdx != -1 && nextStartIdx > nextEndIdx) {
                                        prevLine = next;
                                    }
                                    break;
                                }
                            }
                            if (nextStartIdx != -1) {
                                count++;
                            }
                            if (count != 0) {
                                jsonObj.add(next);
                            }
                        } else {
                            break out;
                        }
                    }
                    map = json.readValue(jsonObj.toString(), Map.class);
                }
            }
            System.out.println(map);
        }
    }
}
