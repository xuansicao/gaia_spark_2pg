package common;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

public class PGTruncate {
    public static void PGtrun(Connection con, String tablename) throws SQLException {
        System.out.println("********************开始清空数据*************************");
        CallableStatement callableStatement = con.prepareCall("truncate table " + tablename);
        callableStatement.execute();
        try {
            if (callableStatement != null) {
                callableStatement.close();
            }
            if (con != null) {
                con.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}
