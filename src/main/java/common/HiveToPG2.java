package common;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import utills.InitSpark;
import utills.JDBCUtil;
import utills.PGProperties;

import java.util.Properties;

public class HiveToPG2 {
    private static String url = null;
    private static Properties pro = null;
    private static String url2 = null;
    private static Properties pro2 = null;

    public static void dataToPG() {

        //参数
        SparkSession spark = InitSpark.initSpark();
        url = JDBCUtil.getUrl();
        pro = PGProperties.getProperties();
        url2 = JDBCUtil.getUrl2();
        pro2 = PGProperties.getProperties2();

        //1.查询 hive 部门表
        String depsql = "select *,unix_timestamp() as data_version from ods_sftm_new.ods_org_dep where bu = 'ky'";
        String dep = "dataman.ods_org_dep";

        //2.查询hive  员工表
        String empsql = "select *,unix_timestamp() as data_version from ods_sftm_new.ods_org_emp where bu = 'ky'";
        String emp = "dataman.ods_org_dep";

        //3.查询hive 经销商通路表
        String custsql = "select *,unix_timestamp() as data_version from dwd_sftm.dwd_cust";
        String cust = "dataman.ods_org_dep";

        //写数据
        appendData(spark, depsql, url, url2, dep, pro, pro2);
        appendData(spark, empsql, url, url2, emp, pro, pro2);
        appendData(spark, custsql, url, url2, cust, pro, pro2);

        spark.close();
    }

    public static void appendData(SparkSession spark, String sql, String url, String url2, String tablename, Properties pro, Properties pro2) {
        Dataset<Row> DF = spark.sql(sql).persist(StorageLevel.MEMORY_AND_DISK_SER());
        DF.write().mode("append").jdbc(url, tablename, pro);
        DF.write().mode("append").jdbc(url2, tablename, pro2);
        DF.unpersist();

    }
}
