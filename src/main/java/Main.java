import org.apache.commons.cli.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;

import util.CreateCSV;
import util.Log;
import util.ConfPar;
import util.Query;

public class Main {

    private final static String confp = "p";
    private final static String tabp = "t";
    private final static String help = "h";
    private final static String outputcsv = "oc";
    private final static String outputblobdir = "ob";

    private static void printHelp(Options options, Optional<String> par, boolean notfound) {
        HelpFormatter formatter = new HelpFormatter();
        String header = "Export XML data from Oracle table (2021/12/13) ";
        if (par.isPresent()) header = " " + par.get() + (notfound ? " not found in the arg list" : "");
        formatter.printHelp(header, options);
        System.exit(4);
    }

    private static Connection connect(String url, String user, String password) throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    private static Options prepareOptions() {
        Options options = new Options();
        options.addOption(confp, true, "Configuration file");
        options.addOption(tabp, true, "Table name");
        options.addOption(outputcsv, true, "(Optional) CVS text file");
        options.addOption(outputblobdir, true, "(Optional) Blob directory");
        return options;
    }

    public static void main(String[] args) {
        Options options = prepareOptions();
        HelpFormatter formatter = new HelpFormatter();
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            Log.severe("Invalid command line parameters", e,false);
            printHelp(options,Optional.empty(),false);
        }
        if (cmd.hasOption(help)) printHelp(options, Optional.empty(), false);
        if (!cmd.hasOption(confp)) printHelp(options, Optional.of(confp), true);
        if (!cmd.hasOption(tabp)) printHelp(options, Optional.of(tabp), true);
        Optional<String> ocsv = cmd.hasOption(outputcsv) ? Optional.of(cmd.getOptionValue(outputcsv)) : Optional.empty();
        Optional<String> odir = cmd.hasOption(outputblobdir) ? Optional.of(cmd.getOptionValue(outputblobdir)) : Optional.empty();
        Log.info(String.format("Output directory: %s", odir.isPresent() ? odir.get() : "created automatically"));
        Log.info(String.format("Output delimited file: %s", ocsv.isPresent() ? ocsv.get() : "created automatically"));
        ConfPar conf = new ConfPar();
        Log.info("Reading " + cmd.getOptionValue(confp));
        conf.load(cmd.getOptionValue(confp));
        Log.info("Connecting ...");
        String tablename = cmd.getOptionValue(tabp);
        try (Connection con = connect(conf.getURL(), conf.getUser(), conf.getPassword())) {
            Log.info("Connected");
            Log.info("Calculating number of rows in " + tablename);
            Long l = Query.numofRecords(con, tablename);
            Log.info("Number of rows:" + l);
            CreateCSV.run(con, conf, tablename, l, ocsv, odir);
        } catch (SQLException | IOException ex) {
            Log.severe("Failed connecting to source database", ex);
        }
    }
}
