package com.alibaba.datax.plugin.writer.logrecorder.util;

import com.alibaba.datax.plugin.rdbms.util.DataBaseType;

/**
 * Created by YQ on 2017/11/14.
 */
public class MatchDatabaseTypeUtil {

    //默认给一个MySQL
    private static DataBaseType DATABASE_TYPE = DataBaseType.MySql;

    public static  DataBaseType getDatabaseType(String readerOrWriterStr){
        if ("".equals(readerOrWriterStr)){
            return null;
        }
        switch (readerOrWriterStr){
            default:
                DATABASE_TYPE = DataBaseType.MySql;
                break;
            case "mysqlreader":
                DATABASE_TYPE = DataBaseType.MySql;
                break;
            case "mysqlwriter":
                DATABASE_TYPE = DataBaseType.MySql;
                break;
            case "oraclereader":
                DATABASE_TYPE = DataBaseType.Oracle;
                break;
            case "oraclewriter":
                DATABASE_TYPE = DataBaseType.Oracle;
                break;
            case "postgresqlreader":
                DATABASE_TYPE = DataBaseType.PostgreSQL;
                break;
            case "postgresqlwriter":
                DATABASE_TYPE = DataBaseType.PostgreSQL;
                break;
            case "sqlserverreader":
                DATABASE_TYPE = DataBaseType.SQLServer;
                break;
            case "sqlserverwriter":
                DATABASE_TYPE = DataBaseType.SQLServer;
                break;
            case "rdbmsreader":
                DATABASE_TYPE = DataBaseType.RDBMS;
                break;
            case "rdbmswriter":
                DATABASE_TYPE = DataBaseType.RDBMS;
                break;
        }

        return  DATABASE_TYPE;
    }



}
