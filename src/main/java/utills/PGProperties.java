package utills;

import java.sql.Connection;
import java.util.Properties;

public class PGProperties {

    private static Properties pro = null;

    public static Properties getProperties() {
        pro = new Properties();
        pro.put("user", JDBCUtil.getUser());
        pro.put("password", JDBCUtil.getPassword());
        pro.put("driver", JDBCUtil.getDriver());

        return pro;
    }

    public static Properties getProperties2() {
        pro = new Properties();
        pro.put("user2", JDBCUtil.getUser2());
        pro.put("password2", JDBCUtil.getPassword2());
        pro.put("driver2", JDBCUtil.getDriver());

        return pro;
    }

}
