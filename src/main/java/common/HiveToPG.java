package common;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class HiveToPG {
    static final String URL = "jdbc:postgresql://192.168.0.57:5432/KSF_DRINK?prepareThreshold=0";
    static final Properties PRO = utills.ConfigPG.getPG();
    static final String URL2 = "jdbc:postgresql://192.168.0.108:5432/KSF_DRINK?prepareThreshold=0";
    static final Properties PRO2 = utills.ConfigPG.get2PG();

    public static void ToPG() {

        //清空表数据
        pgTrun(URL, PRO, "dataman.ods_org_dep");
        pgTrun(URL, PRO, "dataman.ods_org_emp");
        pgTrun(URL, PRO, "dataman.dwd_cust");

        pgTrun(URL2, PRO2, "dataman.ods_org_dep");
        pgTrun(URL2, PRO2, "dataman.ods_org_emp");
        pgTrun(URL2, PRO2, "dataman.dwd_cust");


        //spak初始化
        SparkSession spark = utills.InitSpark.initSpark();

        //1.查询 hive 部门表
        String depsql = "select *,unix_timestamp() as data_version from ods_sftm_new.ods_org_dep where bu = 'ky'";
        Dataset<Row> depDF = spark.sql(depsql).persist();

        //2.查询hive  员工表
        String empsql = "select *,unix_timestamp() as data_version from ods_sftm_new.ods_org_emp where bu = 'ky'";
        Dataset<Row> empDF = spark.sql(empsql).persist(StorageLevel.MEMORY_AND_DISK_SER());

        //3.查询hive 经销商通路表
        String custsql = "select *,unix_timestamp() as data_version from dwd_sftm.dwd_cust";
        Dataset<Row> custDF = spark.sql(custsql).persist(StorageLevel.MEMORY_AND_DISK_SER());


        //写入pg 部门表
        depDF.write().mode("append").jdbc(URL, "dataman.ods_org_dep", PRO);
        depDF.write().mode("append").jdbc(URL2, "dataman.ods_org_dep", PRO2);
        depDF.unpersist();

        //hive写入pg 员工表
        empDF.write().mode("append").jdbc(URL, "dataman.ods_org_emp", PRO);
        empDF.write().mode("append").jdbc(URL2, "dataman.ods_org_emp", PRO2);
        empDF.unpersist();

        //hive写入pg 经销商通路表
        custDF.write().mode("append").jdbc(URL, "dataman.dwd_cust", PRO);
        custDF.write().mode("append").jdbc(URL2, "dataman.dwd_cust", PRO2);
        custDF.unpersist();


        spark.close();

    }


    public static void pgTrun(String URL, Properties PRO, String tablename) {

        try {
            Class.forName("org.postgresql.Driver");
            Connection con1 = DriverManager.getConnection(URL, PRO);
            CallableStatement calls = con1.prepareCall("truncate table " + tablename);
            calls.execute();
            calls.close();
            con1.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
