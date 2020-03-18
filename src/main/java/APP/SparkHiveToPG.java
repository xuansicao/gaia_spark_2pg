package APP;

import common.HiveToPG;
import common.HiveToPG2;
import common.PGTruncate;
import jdk.nashorn.internal.scripts.JD;
import utills.JDBCUtil;
import utills.PGProperties;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ResourceBundle;

public class SparkHiveToPG {

    public static void main(String[] args) {

        ResourceBundle bundle = ResourceBundle.getBundle("jdbcPg");
        String url = bundle.getString("url");
        System.out.println(url);


//        //1.获取连接
//        Connection con = JDBCUtil.getConnection();
//        Connection con2 = JDBCUtil.getConnection2();
//
//        //2.清空PG 57表
//        PGTruncate.PGtrun(con,"dataman.ods_org_dep");
//        PGTruncate.PGtrun(con,"dataman.ods_org_emp");
//        PGTruncate.PGtrun(con,"dataman.dwd_cust");
//        //清空PG 108表
//        PGTruncate.PGtrun(con2,"dataman.ods_org_dep");
//        PGTruncate.PGtrun(con2,"dataman.ods_org_emp");
//        PGTruncate.PGtrun(con2,"dataman.dwd_cust");
//
//        if (con != null) {
//            con.close();
//        }
//
//        if (con2 != null) {
//            con2.close();
//        }
//
//
//        //3.**************************每天全量 hive 导入 pg***************************
//        // 1.部门表--康饮   2.员工表--康饮   3.cust表--康面、康饮、白饮
//
//        //HiveToPG.ToPG();
//
//        HiveToPG2.dataToPG();

    }
}
