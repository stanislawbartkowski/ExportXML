import org.apache.commons.cli.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;

import util.*;

public class Main {

    private final static String confp = "p";
    private final static String tabp = "t";
    private final static String help = "h";
    private final static String outputcsv = "oc";
    private final static String outputblobfile = "ob";
    private final static String destt = "d";
    private final static String silent = "s";
    private final static String query = "q";

    private static void printHelp(Options options, Optional<String> par, boolean notfound) {
        HelpFormatter formatter = new HelpFormatter();
        String header = "Export XML data from Oracle table (2021/12/21) ";
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
        options.addOption(tabp, true, "Source table name");
        options.addOption(outputcsv, true, "(Optional) CVS text file");
        options.addOption(outputblobfile, true, "(Optional) Blob path name");
        options.addOption(destt, true, "(Optional) Name of the destination table");
        options.addOption(silent, false, "(Optional) Silent mode");
        options.addOption(query, true, "(Optional) Extraction query");
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
            Log.severe("Invalid command line parameters", e, false);
            printHelp(options, Optional.empty(), false);
        }
        if (cmd.hasOption(help)) printHelp(options, Optional.empty(), false);
        if (!cmd.hasOption(confp)) printHelp(options, Optional.of(confp), true);
        if (!cmd.hasOption(tabp)) printHelp(options, Optional.of(tabp), true);
        Optional<String> ocsv = cmd.hasOption(outputcsv) ? Optional.of(cmd.getOptionValue(outputcsv)) : Optional.empty();
        Optional<String> oblob = cmd.hasOption(outputblobfile) ? Optional.of(cmd.getOptionValue(outputblobfile)) : Optional.empty();
        Optional<String> oquery = cmd.hasOption(query) ? Optional.of(cmd.getOptionValue(query)) : Optional.empty();
        Optional<String> dtable = cmd.hasOption(destt) ? Optional.of(cmd.getOptionValue(destt)) : Optional.empty();
        boolean silentmode = cmd.hasOption(silent);
        Log.info(String.format("Output directory: %s", oblob.isPresent() ? oblob.get() : "created automatically"));
        Log.info(String.format("Output delimited file: %s", ocsv.isPresent() ? ocsv.get() : "created automatically"));
        Log.info(silentmode ? "Silent mode, no progress indicator on stdout": "Not silent, progress indicator on output");
        Log.info(String.format("Extraction query %s",oquery.isPresent() ? oquery.get() : " (default)"));
        if (dtable.isPresent())
            Log.info(String.format("Data is moved to : %s CSV delimited NOT created", dtable.get()));
        else Log.info("Text delimited file will be created");
        ConfPar conf = new ConfPar();
        Log.info("Reading " + cmd.getOptionValue(confp));
        conf.load(cmd.getOptionValue(confp), dtable.isPresent());
        Log.info("Connecting ...");
        String tablename = cmd.getOptionValue(tabp);
        try (Connection con = connect(conf.getURL(), conf.getUser(), conf.getPassword())) {
            Optional<Connection> dconn = dtable.isPresent() ? Optional.of(connect(conf.getDestURL(), conf.getDestUser(), conf.getDestPassword())) : Optional.empty();

            Log.info("Connected");
            Optional<Long> l = Optional.empty();
            if (silentmode) Log.info("Do not calculate numner of rows");
            else {
                Log.info("Calculating number of rows in " + tablename);
                l = Optional.of(Query.numofRecords(con, tablename));
                Log.info("Number of rows:" + l.get());
            }

            if (dtable.isPresent())
                MoveRows.run(con, conf, tablename, l, silentmode,dconn.get(), dtable.get(),oquery);
            else CreateCSV.run(con, conf, tablename, l, silentmode, ocsv, oblob,oquery);

            if (dconn.isPresent()) dconn.get().close();
        } catch (SQLException | IOException ex) {
            Log.severe("Failed during the connection to source or target database or while running SQL statement", ex);
        }
    }
}
