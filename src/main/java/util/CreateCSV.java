package util;

import java.io.*;
import java.nio.CharBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Optional;


public class CreateCSV {

    private final static String blobname = "BLOB";

    private static class RunCSV extends MoveData {

        private byte[] buf = new byte[1000];

        private final OutputStream blobwriter;
        private final PrintStream writer;
        private final ConfPar par;
        private long blobcounter = 0;


        RunCSV(OutputStream blobwriter, PrintStream writer, ConfPar par) {
            this.blobwriter = blobwriter;
            this.writer = writer;
            this.par = par;
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
            writer.print(blobname);
            writer.print('.');
            writer.print(blobcounter);
            writer.print('.');
            writer.print(bcounter);
            writer.println('/');
            blobcounter += bcounter;
        }
    }


    public static void run(Connection conn, ConfPar par, String tablename, long recno, Optional<String> ocsv, Optional<String> oblob) throws SQLException, IOException {

        File outdir = new File(par.getDir());

        Log.info("Check directory " + outdir.getCanonicalPath());
        outdir.mkdirs();
        if (!outdir.isDirectory()) Log.severe(outdir.getAbsolutePath() + " not a directory");
        File outtext = ocsv.isPresent() ? new File(ocsv.get()) : new File(outdir, tablename + ".csv");
        Log.info("Writing to delimited file " + outtext.getAbsolutePath());

        File blobdir = oblob.isPresent() ? new File(oblob.get()) : new File(outdir, tablename + "blob");
        Log.info("Check blob diretory " + blobdir.getAbsolutePath());
        blobdir.mkdirs();
        if (!blobdir.isDirectory()) Log.severe(blobdir.getAbsolutePath() + " not a directory");
        File blobfile = new File(blobdir, blobname);
        Log.info("Blob content to " + blobfile.getAbsolutePath());

        long counter = 0;
        Log.info("Starting extracting table to delimited file");

        try (OutputStream blobwriter = new FileOutputStream(blobfile);
             PrintStream writer = new PrintStream(outtext)) {

            RunCSV run = new RunCSV(blobwriter, writer, par);
            run.run(conn,par,tablename,recno);
        }
    }
}
