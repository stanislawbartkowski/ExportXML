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
        long blobcounter = 0;
        Log.info("Starting extracting table to delimited file");
        String querystmt = "SELECT " + par.getIdCol() + "," + par.getXmlCol() + " FROM " + tablename;
        if (par.getQuery() != null) {
            querystmt = String.format(par.getQuery(), tablename);
        }
        if (par.getWhere() != null) querystmt = querystmt + " WHERE " + par.getWhere();
        boolean asblob = par.readXmlasblob();
        try (ResultSet res = Query.runStatement(conn, querystmt);
             OutputStream blobwriter = new FileOutputStream(blobfile);
             PrintStream writer = new PrintStream(outtext)) {
            byte[] buf = new byte[1000];
            while (res.next()) {
                String id = res.getString(par.getIdCol());
                int bcounter = 0;
                if (asblob) {
                    SQLXML xml = res.getSQLXML(par.getXmlCol());
                    try (InputStream r = xml.getBinaryStream()) {

                        while (true) {
                            int i = r.read(buf);
                            if (i == -1) break;
                            blobwriter.write(buf, 0, i);
                            bcounter += i;
                        }
                    }
                } else {
                    String xml = res.getString(par.getXmlCol()) + System.lineSeparator();
                    blobwriter.write(xml.getBytes());
                    bcounter = xml.getBytes().length;
                }
                if (counter == 0) System.out.println("First row is arriving");
                counter++;
                if (counter % par.getCounter() == 0) {
                    String info = String.format("%d%% %d (%d)", counter * 100 / recno, counter, recno);
                    String s = String.format("\r %-50s", info);
                    System.out.print(s);
//                    System.out.println((counter * 100 / recno) + "% " + counter + " (" + recno + ")");
                }
                // prepare CSV text line
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
        System.out.println();
        Log.info("Finished");
    }
}
