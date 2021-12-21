package util;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Optional;

// Visitor pattern

abstract class MoveData {

    abstract void acceptrow(String id, Optional<InputStream> i, byte[] vals) throws IOException, SQLException;

    private static InputStream toInputStream(Reader reader) throws IOException {

        char[] charBuffer = new char[1024];
        StringBuilder builder = new StringBuilder();

        int read;
        while ((read = reader.read(charBuffer, 0, charBuffer.length)) != -1) {
            builder.append(charBuffer, 0, read);
        }

        return new ByteArrayInputStream(builder.toString().getBytes(StandardCharsets.UTF_8));
    }

    void run(Connection conn, ConfPar par, String tablename, Optional<Long> recno, boolean silentmode, Optional<String> equery) throws SQLException, IOException {

        long counter = 0;
        // default query
        String query = "SELECT " + par.getIdCol() + "," + par.getXmlCol() + " FROM %s";
        if (equery.isPresent()) query = equery.get();
        else if (par.getQuery() != null) query = par.getQuery();
        String querystmt = String.format(query, tablename);
        if (par.getWhere() != null) querystmt = querystmt + " WHERE " + par.getWhere();
        boolean asblob = par.readXmlasblob();
        try (ResultSet res = Query.runStatement(conn, querystmt)) {
            int xmlpos;
            String xmlname = par.getXmlCol().toLowerCase();
            Log.info("Determine column type for " + par.getXmlCol());
            for (xmlpos = 1; xmlpos <= res.getMetaData().getColumnCount(); xmlpos++)
                if (xmlname.equals(res.getMetaData().getColumnName(xmlpos).toLowerCase())) break;

            if (xmlpos > res.getMetaData().getColumnCount()) {
                Log.severe(String.format("Column %s does not exist in the query ", par.getXmlCol()));
            }
            int coltype = res.getMetaData().getColumnType(xmlpos);
            boolean clob = coltype == Types.CLOB;
            Log.info(clob ? "CLOB column discovered" : "XML column expected");
            while (res.next()) {
                String id = res.getString(par.getIdCol());
                if (asblob) {
                    InputStream is;
                    if (clob) {
                        Clob cl = res.getClob(xmlpos);
                        is = toInputStream(cl.getCharacterStream());
                    } else {
                        SQLXML xml = res.getSQLXML(xmlpos);
                        is = xml.getBinaryStream();
                    }
                    acceptrow(id, Optional.of(is), null);
                } else {
                    String xml = res.getString(xmlpos) + System.lineSeparator();
                    acceptrow(id, Optional.empty(), xml.getBytes());
                }
                if (counter == 0) Log.info("First row is arriving");
                counter++;
                if (!silentmode && counter % par.getCounter() == 0) {
                    String info = String.format("%d%% %d (%d)", counter * 100 / recno.get(), counter, recno.get());
                    String s = String.format("\r %-50s", info);
                    System.out.print(s);
                }
            }
        }
        System.out.println();
        Log.info("Finished");
    }
}

