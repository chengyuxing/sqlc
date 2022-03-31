package rabbit.sql.connsole.test;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(separators = "=", commandDescription = "support Query, DDL, DML, Transaction")
public class ArgTests {
    @Parameter(names = {"-h", "--help"}, description = "get help.", help = true)
    String help;
    @Parameter(names = {"-v", "--version"}, description = "1.0.3")
    String version;
    @Parameter(names = "-u", description = "jdbc url, e.g.: -ujdbc:postgresql://...", order = 1)
    String url;
    @Parameter(names = "-n", description = "database username.", order = 2)
    String username;
    @Parameter(names = "-p", description = "database password.", order = 3, password = true)
    String password;

    public static void main(String[] args) {
        ArgTests argTests = new ArgTests();
        JCommander commander = JCommander.newBuilder()
                .programName("Command Line sql tool!")
                .addObject(argTests)
                .build();
        if (args.length == 0) {
            commander.usage();
            System.exit(0);
        }
        commander.parse(args);
        argTests.run();

    }

    public void run() {
        System.out.println(url);
        System.out.println(password);
    }
}
