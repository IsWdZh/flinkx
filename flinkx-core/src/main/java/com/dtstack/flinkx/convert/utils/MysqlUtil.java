package com.dtstack.flinkx.convert.utils;



import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * mysql工具类
 *
 * @author liuwenjie
 * @date 2019/10/21 17:52
 */
public class MysqlUtil {
    public static Logger LOG = LoggerFactory.getLogger(MysqlUtil.class);
    private static String IP = "192.168.108.37";
    private static Integer PORT = 3306;
    private static String DATABASE = "data_sync";
    private static String USER = "root";
    private static String PASSWORD = "Pass1234";

    static Connection init(){
        try {
            LOG.info("初始化 mysql---");
            LOG.info("IP---{}",IP);
            Class.forName("com.mysql.jdbc.Driver");
            Connection mConnect = DriverManager.getConnection("jdbc:mysql://" + IP + ":" + PORT + "/" + DATABASE +"?useUnicode=true&characterEncoding=UTF-8", USER, PASSWORD);
            return mConnect;
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    static Connection init(String ip,Integer port,String dbName,String user,String password){
        try {
            LOG.info("初始化 mysql---");
            LOG.info("IP---{}",ip);
            Class.forName("com.mysql.jdbc.Driver");
            Connection mConnect = DriverManager.getConnection("jdbc:mysql://" + ip + ":" + port + "/" + dbName +"?useUnicode=true&characterEncoding=UTF-8&useInformationSchema=true", user, password);
            return mConnect;
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    static void close(Connection connection,Statement statement){
        if(null!=statement){
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if(null!=connection){
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void addTbDagRun(Map<String, Object> paramMap) {
        //INSERT INTO tb_dag_run(dag_id,exec_date,start_date,end_date,dag_status,remark) VALUES ('1','2019-10-21 01:33:49','2019-10-21 01:33:49','2019-10-21 01:33:49',10,'22')
        Object dagId = paramMap.get("dagId");
        Object execDate = paramMap.get("execDate");
        Object startDate = paramMap.get("startDate");
        Object endDate = paramMap.get("endDate");
        Object dagStatus = paramMap.get("dagStatus");
        Object remark = paramMap.get("remark");
        Statement statement=null;
        Connection connection =null ;
        try {
            connection=init();
            statement = connection.createStatement();
            String sql = "INSERT INTO tb_dag_run(dag_id, exec_date, start_date,end_date,dag_status,remark) VALUES (" +
                    "'" + dagId + "'," +
                    "'" + execDate + "'," +
                    "'" + startDate + "'," +
                    "'" + endDate + "'," +
                    "'" + dagStatus + "'," +
                    "'" + remark + "')";
            LOG.info("插入SQL语句 {}",sql);
            statement.execute(sql);
            LOG.info("插入success {}",paramMap);
        } catch (SQLException e) {
            LOG.info("插入failssss {}", ExceptionUtils.getFullStackTrace(e));
        }finally {
            close(connection,statement);
        }
    }

    public static void addTbDagRun(Map<String, Object> paramMap,Map dbConnectMap) {
        //INSERT INTO tb_dag_run(dag_id,exec_date,start_date,end_date,dag_status,remark) VALUES ('1','2019-10-21 01:33:49','2019-10-21 01:33:49','2019-10-21 01:33:49',10,'22')
        Object dagId = paramMap.get("dagId");
        Object execDate = paramMap.get("execDate");
        Object startDate = paramMap.get("startDate");
        Object endDate = paramMap.get("endDate");
        Object dagStatus = paramMap.get("dagStatus");
        Object remark = paramMap.get("remark");

        //数据库连接信息
        String ip = String.valueOf(dbConnectMap.get("ip"));
        Integer port = Integer.valueOf(String.valueOf(dbConnectMap.get("port")));
        String dbName = String.valueOf(dbConnectMap.get("dbName"));
        String user = String.valueOf(dbConnectMap.get("user"));
        String password = String.valueOf(dbConnectMap.get("password"));

        Statement statement=null;
        Connection connection =null ;
        try {
            connection=init(ip,port,dbName,user,password);
            statement = connection.createStatement();
            String sql = "INSERT INTO tb_dag_run(dag_id, exec_date, start_date,end_date,dag_status,remark) VALUES (" +
                    "'" + dagId + "'," +
                    "'" + execDate + "'," +
                    "'" + startDate + "'," +
                    "'" + endDate + "'," +
                    "'" + dagStatus + "'," +
                    "'" + remark + "')";
            LOG.info("插入SQL语句 {}",sql);
            statement.execute(sql);
            LOG.info("插入success {}",paramMap);
        } catch (SQLException e) {
            LOG.info("插入failssss {}", ExceptionUtils.getFullStackTrace(e));
        }finally {
            close(connection,statement);
        }
    }

    public static void updateTbDagRun(Map<String, Object> paramMap,Map dbConnectMap) {
        //INSERT INTO tb_dag_run(dag_id,exec_date,start_date,end_date,dag_status,remark) VALUES ('1','2019-10-21 01:33:49','2019-10-21 01:33:49','2019-10-21 01:33:49',10,'22')
        Object dagId = paramMap.get("dagId");
        Object execDate = paramMap.get("execDate");
        Object startDate = paramMap.get("startDate");
        Object endDate = paramMap.get("endDate");
        Object dagStatus = paramMap.get("dagStatus");
        Object remark = paramMap.get("remark");

        //数据库连接信息
        String ip = String.valueOf(dbConnectMap.get("ip"));
        Integer port = Integer.valueOf(String.valueOf(dbConnectMap.get("port")));
        String dbName = String.valueOf(dbConnectMap.get("dbName"));
        String user = String.valueOf(dbConnectMap.get("user"));
        String password = String.valueOf(dbConnectMap.get("password"));

        //UPDATE tb_dag_run SET end_date='2019-10-21 01:33:48' AND DAG_STATUS=2 AND remark='adsf' WHERE dag_id='1' AND EXEC_DATE='2019-10-21 01:33:49'
        Statement statement=null;
        Connection connection =null ;
        try {
            connection=init(ip,port,dbName,user,password);
            statement = connection.createStatement();
            String sql = "UPDATE tb_dag_run " +
                    "SET " +
                    "end_date='"+endDate+"',"+
                    "dag_status="+dagStatus+"," +
                    "remark='"+remark+"'"+
                    "WHERE dag_id='"+dagId+"' AND exec_date='"+execDate+"'";
            LOG.info("修改SQL语句 {}",sql);
            statement.execute(sql);
            LOG.info("修改success {}",paramMap);
        } catch (SQLException e) {
            LOG.info("修改failssss {}", ExceptionUtils.getFullStackTrace(e));
        }finally {
            close(connection,statement);
        }
    }

    public static void updateTbDagRun(Map<String, Object> paramMap) {
        //INSERT INTO tb_dag_run(dag_id,exec_date,start_date,end_date,dag_status,remark) VALUES ('1','2019-10-21 01:33:49','2019-10-21 01:33:49','2019-10-21 01:33:49',10,'22')
        Object dagId = paramMap.get("dagId");
        Object execDate = paramMap.get("execDate");
        Object startDate = paramMap.get("startDate");
        Object endDate = paramMap.get("endDate");
        Object dagStatus = paramMap.get("dagStatus");
        Object remark = paramMap.get("remark");

        //UPDATE tb_dag_run SET end_date='2019-10-21 01:33:48' AND DAG_STATUS=2 AND remark='adsf' WHERE dag_id='1' AND EXEC_DATE='2019-10-21 01:33:49'
        Statement statement=null;
        Connection connection =null ;
        try {
            connection=init();
            statement = connection.createStatement();
            String sql = "UPDATE tb_dag_run " +
                    "SET " +
                    "end_date='"+endDate+"',"+
                    "dag_status="+dagStatus+"," +
                    "remark='"+remark+"'"+
                    "WHERE dag_id='"+dagId+"' AND exec_date='"+execDate+"'";
            LOG.info("修改SQL语句 {}",sql);
            statement.execute(sql);
            LOG.info("修改success {}",paramMap);
        } catch (SQLException e) {
            LOG.info("修改failssss {}", ExceptionUtils.getFullStackTrace(e));
        }finally {
            close(connection,statement);
        }
    }

//    wangcaiwen
    public static void updateTbMetadata(Map<String, Object> paramMap,Map dbConnectMap) {
        Object colId = paramMap.get("COL_ID");
        Object addValue = paramMap.get("ADD_VALUE");

        //数据库连接信息
        String ip = String.valueOf(dbConnectMap.get("ip"));
        Integer port = Integer.valueOf(String.valueOf(dbConnectMap.get("port")));
        String dbName = String.valueOf(dbConnectMap.get("dbName"));
        String user = String.valueOf(dbConnectMap.get("user"));
        String password = String.valueOf(dbConnectMap.get("password"));

        Statement statement=null;
        Connection connection =null ;
        try {
            connection=init(ip,port,dbName,user,password);
            statement = connection.createStatement();
            String sql = "UPDATE tb_metadata " +
                    "SET " +
                    "ADD_VALUE='"+addValue+"'"+
                    "WHERE COL_ID='"+colId+"'";
            LOG.info("修改TbMetadataSQL语句 {}",sql);
            statement.execute(sql);
            LOG.info("修改success {}",paramMap);
        } catch (SQLException e) {
            LOG.info("修改failssss {}", ExceptionUtils.getFullStackTrace(e));
        }finally {
            close(connection,statement);
        }
    }


    public static void updateError(Map<String, Object> paramMap,Map dbConnectMap) {
        //INSERT INTO tb_dag_run(dag_id,exec_date,start_date,end_date,dag_status,remark) VALUES ('1','2019-10-21 01:33:49','2019-10-21 01:33:49','2019-10-21 01:33:49',10,'22')
        Object dagId = paramMap.get("dagId");
        Object dagStatus = paramMap.get("dagStatus");
        Object remark = paramMap.get("remark");
        Object execDate = paramMap.get("execDate");

        //数据库连接信息
        String ip = String.valueOf(dbConnectMap.get("ip"));
        Integer port = Integer.valueOf(String.valueOf(dbConnectMap.get("port")));
        String dbName = String.valueOf(dbConnectMap.get("dbName"));
        String user = String.valueOf(dbConnectMap.get("user"));
        String password = String.valueOf(dbConnectMap.get("password"));

        //UPDATE tb_dag_run SET end_date='2019-10-21 01:33:48' AND DAG_STATUS=2 AND remark='adsf' WHERE dag_id='1' AND EXEC_DATE='2019-10-21 01:33:49'
        Statement statement=null;
        Connection connection =null ;
        try {
            connection=init(ip,port,dbName,user,password);
            statement = connection.createStatement();
            String sql = "UPDATE tb_dag_run " +
                    "SET " +
                    "dag_status="+dagStatus+"," +
                    "remark='"+remark+"'"+
                    " WHERE dag_id='"+dagId+"' AND exec_date='"+execDate+"'";
            LOG.info("修改SQL语句 {}",sql);
            statement.execute(sql);
            LOG.info("修改success {}",paramMap);
        } catch (SQLException e) {
            LOG.info("修改failssss {}", ExceptionUtils.getFullStackTrace(e));
        }finally {
            close(connection,statement);
        }
    }

    public static void updateError(Map<String, Object> paramMap) {
        //INSERT INTO tb_dag_run(dag_id,exec_date,start_date,end_date,dag_status,remark) VALUES ('1','2019-10-21 01:33:49','2019-10-21 01:33:49','2019-10-21 01:33:49',10,'22')
        Object dagId = paramMap.get("dagId");
        Object dagStatus = paramMap.get("dagStatus");
        Object remark = paramMap.get("remark");
        Object execDate = paramMap.get("execDate");
        //UPDATE tb_dag_run SET end_date='2019-10-21 01:33:48' AND DAG_STATUS=2 AND remark='adsf' WHERE dag_id='1' AND EXEC_DATE='2019-10-21 01:33:49'
        Statement statement=null;
        Connection connection =null ;
        try {
            connection=init();
            statement = connection.createStatement();
            String sql = "UPDATE tb_dag_run " +
                    "SET " +
                    "dag_status="+dagStatus+"," +
                    "remark='"+remark+"'"+
                    " WHERE dag_id='"+dagId+"' AND exec_date='"+execDate+"'";
            LOG.info("修改SQL语句 {}",sql);
            statement.execute(sql);
            LOG.info("修改success {}",paramMap);
        } catch (SQLException e) {
            LOG.info("修改failssss {}", ExceptionUtils.getFullStackTrace(e));
        }finally {
            close(connection,statement);
        }
    }

    /**
     *
     * @param dbConnectMap
     * @return
     */
    public static List<String> getColums(Map<String,Object> dbConnectMap){

        //数据库连接信息
        String ip = String.valueOf(dbConnectMap.get("ip"));
        Integer port = Integer.valueOf(String.valueOf(dbConnectMap.get("port")));
        String dbName = String.valueOf(dbConnectMap.get("dbName"));
        String user = String.valueOf(dbConnectMap.get("user"));
        String password = String.valueOf(dbConnectMap.get("password"));

        //表信息和schema信息
        //SELECT * FROM information_schema.`COLUMNS`
        String tableName = String.valueOf(dbConnectMap.get("tableName"));
        String schemaName = String.valueOf(dbConnectMap.get("schemaName"));

        Connection connection = null;
        Statement statement = null;
        ResultSet rs =null;
        try {
            connection=init(ip, port, dbName, user, password);
            statement = connection.createStatement();
            String sql="select column_name from " +"`COLUMNS`" + " where table_name ="+"'" + tableName + "'" + " and table_schema="+"'" + schemaName + "'";
            LOG.info("查询SQL语句 {}",sql);
            rs = statement.executeQuery(sql);
            List<String> ret = new ArrayList<>();
            while(rs.next()) {
                try {
                    ret.add(rs.getString("COLUMN_NAME"));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            return ret;
        } catch (SQLException e) {
            LOG.info("查询failssss {}", ExceptionUtils.getFullStackTrace(e));
        }finally {
            try {
                rs.close();
                close(connection,statement);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public static void main(String[] args) throws SQLException {
        String COL_ID = "2";
        String ADD_VALUE = "500";
        Map<String, Object> paramMap = new HashMap<>(2);
        paramMap.put("COL_ID", COL_ID);
        paramMap.put("ADD_VALUE", ADD_VALUE);

        Map<String,Object> dbConnectMap = new HashMap<>();
        dbConnectMap.put("ip","10.160.32.5");
        dbConnectMap.put("port",3306);
        dbConnectMap.put("user","root");
        dbConnectMap.put("password","Pass1234");
        dbConnectMap.put("dbName","data_sync");

        updateTbMetadata(paramMap,dbConnectMap);
    }
}
