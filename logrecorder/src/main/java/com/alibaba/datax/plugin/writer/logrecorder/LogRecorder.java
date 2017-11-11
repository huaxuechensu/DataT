package com.alibaba.datax.plugin.writer.logrecorder;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.datax.plugin.rdbms.writer.util.WriterUtil;
import com.alibaba.datax.plugin.writer.logrecorder.util.ConfigParser;
import com.alibaba.datax.plugin.writer.logrecorder.util.container.CoreConstant;
import sun.security.krb5.Config;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


//TODO writeProxy
public final class LogRecorder {
    private static DataBaseType DATABASE_TYPE = DataBaseType.MySql;

    private CommonRdbmsWriter.Job commonRdbmsWriterJob;

    public static void main(String[] args) {

       //test(Configuration.newDefault());
       // writeData(Configuration.newDefault());
        //esembleConf();


        List<String> renderedPreSqls = new ArrayList<String>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm.SSS");
        String value = sdf.format(new Date())+"beijing";
        renderedPreSqls.add("insert INTO sys_user VALUES('1','chenyuqg','male','"+value+"')");

        writeData("sys_user",renderedPreSqls);

    }

    public static void esembleConf(){

        //Configuration conf = ConfigParser.parse(CoreConstant.DATAX_LOG2DB_PATH);

        Configuration conf = ConfigParser.parse("./log2DB.json");
        System.out.println("!!!!!!!!!!!!:"+conf.toString());

        //Configuration configuration = ConfigParser.parse(jobPath);
        //Configuration configuration = ConfigParser.parse("/opt/datax/job/mysql2mysql.json");

        //configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, jobId);
        //ConfigurationValidate.doValidate(configuration);


        //Configuration jobInfoConfig = configuration.getConfiguration(CoreConstant.DATAX_JOB_JOBINFO);
        //Configuration writerConfig = configuration.getConfiguration(CoreConstant.TRANSFORMER_PARAMETER_PARAS);


    }



    public static void test(Configuration configuration){
        System.out.println("chenyuqg:这是里logrecorder——begin");
        //System.out.println(configuration.toString());
        System.out.println("chenyuqg:这是里logrecorder——end");

    }

    public static void writeData(String tableName, List<String> renderedPreSqls){
        /*String jdbcUrl = "jdbc:mysql://192.168.189.188:3306/test_target";
        String username = "root";
        String password = "root";
        String name = "mysqlwriter";
        Configuration logDBConf = Configuration.newDefault();
        logDBConf.set(Key.JDBC_URL,jdbcUrl);
        logDBConf.set(Key.USERNAME,username);
        logDBConf.set(Key.PASSWORD,password);
        logDBConf.set("databaseType",name);*/
        //上面几行模拟一个由json转化而来的configuration

        //Configuration cf = ConfigParser.parse(CoreConstant.DATAX_LOG2DB_PATH);
        //Configuration cf = ConfigParser.parseCoreConfig(CoreConstant.DATAX_CONF_PATH);
        //Configuration cf = Configuration.from(CoreConstant.DATAX_LOG2DB_PATH);
      /*  Configuration cf2 = Configuration.from(new File("log2DB.json"));
        System.out.println("************"+cf2.toJSON());
        cf2.getString("jdbc");*/
       /* System.out.println("chenyuqg实际路径是2："+new File("../conf/log2DB.json"));
        try {
            Configuration cf = Configuration.from(new File("../conf/log2DB.json"));
            System.out.println("&&&&&&&&&&"+cf.toJSON());
        }catch (Exception e){
            Configuration cf = Configuration.from(new File(CoreConstant.DATAX_LOG2DB_PATH));
            System.out.println("************"+cf.toJSON());
        }*/

        String path = LogRecorder.class.getClassLoader().getResource(".") .getFile();
        File log2DBParFile = new File(path +"conf"+ File.separator + "log2DB.json");
        Configuration logDBConf = Configuration.from(log2DBParFile);
        System.out.println("修改之后的结果"+logDBConf.toString());
        //Configuration logDBConf = ConfigParser.parse(path +"conf"+ File.separator + "log2DB.json");

        //根据JSON配置，匹配合适的数据库类型
        String dbType = logDBConf.getString("name");
        System.out.println("修改之后的dbType结果"+dbType);
        switch (dbType){
            default:
                DATABASE_TYPE = DataBaseType.MySql;
                break;
            case "mysqlwriter":
                DATABASE_TYPE = DataBaseType.MySql;
                break;
            case "oraclewriter":
                DATABASE_TYPE = DataBaseType.Oracle;
                break;
            case "postgresqlwriter":
                DATABASE_TYPE = DataBaseType.PostgreSQL;
                break;
            case "sqlserverwriter":
                DATABASE_TYPE = DataBaseType.SQLServer;
                break;
            case "rdbmswriter":
                DATABASE_TYPE = DataBaseType.RDBMS;
                break;
        }

        //在这个configuration中加入自定义的变量和值
        logDBConf.set("batchSize","2048");
        logDBConf.set(Key.TABLE,tableName);
        System.out.println("准备获取conn");
        Connection conn = DBUtil.getConnection(DATABASE_TYPE,logDBConf.getString(Key.JDBC_URL),logDBConf.getString(Key.USERNAME),logDBConf.getString(Key.PASSWORD));
        System.out.println("准备执行sql");
        WriterUtil.executeSqls(conn, renderedPreSqls, logDBConf.getString(Key.JDBC_URL), DATABASE_TYPE);
        System.out.println("准备关闭资源");
        DBUtil.closeDBResources(null, null, conn);
        System.out.println("关闭连接");

    }


}
