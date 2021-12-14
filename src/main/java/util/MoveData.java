package util;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Optional;

// Visitor pattern

abstract class MoveData {

    abstract void acceptrow(String id, Optional<InputStream> i, byte[] vals) throws IOException, SQLException;

    void run(Connection conn, ConfPar par, String tablename, long recno) throws SQLException, IOException {

        long counter = 0;
        String querystmt = "SELECT " + par.getIdCol() + "," + par.getXmlCol() + " FROM " + tablename;
        if (par.getQuery() != null) {
            querystmt = String.format(par.getQuery(), tablename);
        }
        if (par.getWhere() != null) querystmt = querystmt + " WHERE " + par.getWhere();
        boolean asblob = par.readXmlasblob();
        try (ResultSet res = Query.runStatement(conn, querystmt)) {
            while (res.next()) {
                String id = res.getString(par.getIdCol());
                if (asblob) {
                    SQLXML xml = res.getSQLXML(par.getXmlCol());
                    acceptrow(id, Optional.of(xml.getBinaryStream()), null);

                } else {
                    String xml = res.getString(par.getXmlCol()) + System.lineSeparator();
                    acceptrow(id, Optional.empty(), xml.getBytes());
                }
                if (counter == 0) Log.info("First row is arriving");
                counter++;
                if (counter % par.getCounter() == 0) {
                    String info = String.format("%d%% %d (%d)", counter * 100 / recno, counter, recno);
                    String s = String.format("\r %-50s", info);
                    System.out.print(s);
                }
            }
        }
        System.out.println();
        Log.info("Finished");
    }
}

