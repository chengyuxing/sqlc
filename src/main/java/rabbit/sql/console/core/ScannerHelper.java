package rabbit.sql.console.core;

import com.github.chengyuxing.common.console.Color;
import com.github.chengyuxing.common.console.Printer;

public class ScannerHelper {
    public static void sprint(String text) {
        String txActiveFlag = StatusManager.txActive.get() ? "*" : "";
        if (!PrintHelper.isWindows) {
            System.out.print(Printer.colorful(txActiveFlag, Color.SILVER) + Printer.colorful(text, Color.PURPLE) + " ");
        } else {
            System.out.print(txActiveFlag + text + " ");
        }
    }

    public static void newLine() {
        sprint("sqlc>");
    }

    public static void append() {
        sprint(">>");
    }

    public static void print(String text) {
        if (!PrintHelper.isWindows) {
            System.out.print(Printer.colorful(text, Color.PURPLE) + " ");
        } else {
            System.out.print(text + " ");
        }
    }
}
