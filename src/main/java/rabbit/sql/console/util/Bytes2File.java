package rabbit.sql.console.util;

import com.github.chengyuxing.common.io.IOutput;

import java.io.IOException;

public class Bytes2File implements IOutput {
    private final byte[] bytes;

    public Bytes2File(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public byte[] toBytes() throws IOException {
        return bytes;
    }
}
