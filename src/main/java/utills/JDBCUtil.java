package utills;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.ResourceBundle;

public class JDBCUtil {
    static String url = null;
    static String user = null;
    static String password = null;
    static String driver = null;

    static String url2 = null;
    static String user2 = null;
    static String password2 = null;

    static Connection con = null;
    static InputStream resourceAsStream = null;

    public static String getUrl() {
        return url;
    }

    public static String getUser() {
        return user;
    }

    public static String getPassword() {
        return password;
    }

    public static String getDriver() {
        return driver;
    }

    public static String getUrl2() {
        return url2;
    }

    public static String getUser2() {
        return user2;
    }

    public static String getPassword2() {
        return password2;
    }

    static {
        try {
            //读取 properties文件
            Properties properties = new Properties();
            ClassLoader classLoader = JDBCUtil.class.getClassLoader();

            //方式一：
            resourceAsStream = classLoader.getResourceAsStream("jdbcPg.properties");
            properties.load(resourceAsStream);

            //方式二：ResourceBundle类
            ResourceBundle bundle = ResourceBundle.getBundle("jdbcPg");
            bundle.getString("url");

           /*
            URL resource = classLoader.getResource("JDBCPg_properties");
            String path = resource.getPath();
            properties.load(new FileInputStream(path));
            */

            //获取PG 57 配置
            url = properties.getProperty("url");
            user = properties.getProperty("user");
            password = properties.getProperty("password");
            driver = properties.getProperty("driver");

            //获取PG 108 配置
            url2 = properties.getProperty("url2");
            user2 = properties.getProperty("user2");
            password2 = properties.getProperty("password2");

            Class.forName(driver);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (resourceAsStream != null) {
                    resourceAsStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    //获取连接
    public static Connection getConnection() {
        try {
            con = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return con;
    }

    public static Connection getConnection2() {
        try {
            con = DriverManager.getConnection(url2, user2, password2);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return con;
    }
}
