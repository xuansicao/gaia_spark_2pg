package utills;

import java.util.Properties;

public class ConfigPG {

    //连接pg
    public static Properties getPG() {
        Properties pro = new Properties();
        pro.put("user", "postgres");
        pro.put("password", "postgis");
        pro.put("driver", "org.postgresql.Driver");

        return pro;

    }

    public static Properties get2PG() {
        Properties pro2 = new Properties();
        pro2.put("user", "postgres");
        pro2.put("password", "postgres@gaia");
        pro2.put("driver", "org.postgresql.Driver");

        return pro2;

    }

}
