package util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;

public class MoveRows {

    private static class RunMove extends MoveData {

        private final PreparedStatement prep;
        private int commitno = 0;

        RunMove(PreparedStatement prep) {
            this.prep = prep;
        }

        @Override
        void acceptrow(String id, Optional<InputStream> i, byte[] vals) throws IOException, SQLException {

            prep.setString(1, id);
            if (i.isPresent()) prep.setBlob(2, i.get());
            else {
                ByteArrayInputStream bu = new ByteArrayInputStream(vals);
                prep.setBlob(2, bu);
            }
            prep.execute();
            commitno++;
            if (commitno > 1000) {
                prep.getConnection().commit();
                commitno = 0;
            }
        }
    }

    public static void run(Connection conn, ConfPar par, String tablename, Optional<Long> recno, boolean silentmode, Connection dcon, String desttablename, Optional<String> eqeury) throws SQLException, IOException {

        String stmt = String.format(par.getDestInsert(), desttablename);
        Log.info(String.format("Target table %s", desttablename));
        Log.info(stmt);
        try (PreparedStatement prep = dcon.prepareStatement(stmt)) {
            RunMove run = new RunMove(prep);
            run.run(conn, par, tablename, recno, silentmode,eqeury);
            // last commit, just in case
            dcon.commit();
        }
    }
}
