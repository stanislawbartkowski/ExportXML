package util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Query {

    static ResultSet runStatement(Connection con, String statement) throws SQLException {
        Log.info(statement);
        return con.prepareStatement(statement).executeQuery();
    }
    public static long numofRecords(Connection con, String tablename) throws SQLException {
        try (ResultSet res = runStatement(con,"SELECT COUNT(*) FROM " + tablename)) {
            res.next();
            return res.getLong(1);
        }
    }
}
