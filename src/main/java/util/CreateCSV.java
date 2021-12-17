package util;

import java.io.*;
import java.nio.CharBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Optional;


public class CreateCSV {

    private final static String defaultblobname = "BLOB";

    private static class RunCSV extends MoveData {

        private byte[] buf = new byte[1000];

        private final OutputStream blobwriter;
        private final PrintStream writer;
        private final ConfPar par;
        private long blobcounter = 0;
        private final String blobfile;


        RunCSV(OutputStream blobwriter, PrintStream writer, ConfPar par, String blobfile) {
            this.blobwriter = blobwriter;
            this.writer = writer;
            this.par = par;
            this.blobfile = blobfile;
        }

        @Override
        void acceptrow(String id, Optional<InputStream> r, byte[] vals) throws IOException {

            int bcounter = 0;

            if (r.isPresent()) {

                while (true) {
                    int i = r.get().read(buf);
                    if (i == -1) break;
                    blobwriter.write(buf, 0, i);
                    bcounter += i;
                }
            } else {
                blobwriter.write(vals);
                bcounter = vals.length;
            }

            writer.print(id);
            writer.print(par.getDel());
            writer.print(blobfile);
            writer.print('.');
            writer.print(blobcounter);
            writer.print('.');
            writer.print(bcounter);
            writer.println('/');
            blobcounter += bcounter;
        }
    }

    private static File extractDir(File f) {
        File parent = f.getParentFile();
        return parent;
    }

    private static String extractFile(File f) {
        return f.getName();
    }

    public static void run(Connection conn, ConfPar par, String tablename, Optional<Long> recno, boolean silentmode, Optional<String> ocsv, Optional<String> oblob, Optional<String> equery) throws SQLException, IOException {

        File outdir = new File(par.getDir());

        Log.info("Check directory " + outdir.getCanonicalPath());
        outdir.mkdirs();
        if (!outdir.isDirectory()) Log.severe(outdir.getAbsolutePath() + " not a directory");
        File outtext = ocsv.isPresent() ? new File(ocsv.get()) : new File(outdir, tablename + ".csv");
        Log.info("Writing to delimited file " + outtext.getAbsolutePath());

        // blob file and dir
        File blobfile = oblob.isPresent() ? new File(oblob.get()) : new File(new File(outdir, tablename + "blob"), defaultblobname);
        File blobdir = extractDir(blobfile);
        blobdir.mkdirs();
        if (!blobdir.isDirectory()) Log.severe(blobdir.getAbsolutePath() + " not a directory");
        Log.info("Check blob directory " + blobdir.getAbsolutePath());
        String blobname = extractFile(blobfile);
        Log.info("Blob content to " + blobfile.getAbsolutePath());


        long counter = 0;
        Log.info("Starting extracting table to delimited file");

        try (OutputStream blobwriter = new FileOutputStream(blobfile);
             PrintStream writer = new PrintStream(outtext)) {

            RunCSV run = new RunCSV(blobwriter, writer, par,blobname);
            run.run(conn, par, tablename, recno, silentmode, equery);
        }
    }
}
