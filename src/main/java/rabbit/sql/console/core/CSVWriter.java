package rabbit.sql.console.core;

import rabbit.common.io.DSVWriter;

import java.io.OutputStream;

public class CSVWriter extends DSVWriter {
    /**
     * 构造函数
     *
     * @param out 输出流
     */
    public CSVWriter(OutputStream out) {
        super(out);
    }

    @Override
    protected String delimiter() {
        return ", ";
    }
}
