package com.dtstack.flinkx.convert.utils;

import com.dtstack.flinkx.config.*;
import net.minidev.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author: Zhou Wendi
 * @Date: 2020/12/9
 */
public class TaskExec {

    public static Logger logger = LoggerFactory.getLogger(TaskExec.class);

    /**
     * 新增任务执行记录
     *
     * @param jobIdString  jobid
     * @param execDate     任务执行时间
     * @param dbConnectMap 业务库（mysql）链接信息
     * @return
     */
    public static Map<String, Object> addTbDagRun(String jobIdString, String execDate, Map dbConnectMap) {
        String dagId = jobIdString;
        String startDate = execDate;
        String endDate = execDate;
        Integer dagStatus = 1;
        String remark = "ddd";

        //为null随机生成一个jobId
        if (null == dagId) {
            dagId = UUID.randomUUID().toString();
        }

        Map<String, Object> paramMap = new HashMap<>(8);
        paramMap.put("dagId", dagId);
        paramMap.put("execDate", execDate);
        paramMap.put("startDate", startDate);
        paramMap.put("endDate", endDate);
        paramMap.put("dagStatus", dagStatus);
        paramMap.put("remark", remark);

        MysqlUtil.addTbDagRun(paramMap, dbConnectMap);

        return paramMap;
    }

    /**
     * 修改任务执行记录
     *
     * @param paramMap        任务基础信息
     * @param sourceTableName 源表名
     * @param targetTableName 目标表名
     * @param startTime       开始时间
     * @param dbConnectMap    业务库（mysql）链接信息
     */
    public static void updateTbDagRun(Map<String, Object> paramMap, String sourceTableName, String
            targetTableName, long startTime, Map dbConnectMap) {
        // Map<String, Object> taskDetailMap = ResultPrintUtil.getTaskDetailMap();
        Map<String, Object> taskDetailMap = ResultUtil.getTaskDetailMap();

        logger.info("taskDetailMap：" + taskDetailMap);

        //累计错误记录数
        Long nErrors = (Long) taskDetailMap.get("nErrors");
        //累计过滤条数
        Long numWrite = (Long) taskDetailMap.get("numWrite") - nErrors;
        Long numFilter = ((Long) taskDetailMap.get("numRead")) - numWrite;
        String costTime = (System.currentTimeMillis() - startTime) + " ms";
        taskDetailMap.put("numWrite", numWrite);
        taskDetailMap.put("numFilter", numFilter);
        taskDetailMap.put("sourceTableName", sourceTableName);
        taskDetailMap.put("targetTableName", targetTableName);
        taskDetailMap.put("costTime", costTime);


        Integer dagStatus = 0;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String endDate = simpleDateFormat.format(System.currentTimeMillis());
        paramMap.put("endDate", endDate);
        paramMap.put("dagStatus", dagStatus);
        paramMap.put("remark", JSONObject.toJSONString(taskDetailMap));

        MysqlUtil.updateTbDagRun(paramMap, dbConnectMap);
    }

    /**
     * 修改Metadata记录
     *
     * @param metaId       元数据ID
     * @param maxValue     当前表中增量字段对应的最大值
     * @param dbConnectMap 业务库（mysql）链接信息
     * @Author wangcaiwen
     */
    public static Map<String, Object> updateTbMetadata(String metaId, String maxValue, Map dbConnectMap) {
        Map<String, Object> paramMap = new HashMap<>(2);
        paramMap.put("COL_ID", metaId);
        paramMap.put("ADD_VALUE", maxValue);
        logger.info("paramMap：" + paramMap);
        MysqlUtil.updateTbMetadata(paramMap, dbConnectMap);
        return paramMap;
    }


    /**
     * 新增
     * @param writerName
     * @param writerConfigParameter
     * @return
     */
    public static String getTargetTableName(String writerName, WriterConfig.ParameterConfig writerConfigParameter) {
        String targetTableName = null;
        // if ("hdfswriter".equalsIgnoreCase(writerName)) {
        //     String path = writerConfigParameter.getPath();
        //     logger.info("===path {}", path);
        //     if (path.contains("=")) {
        //         List<String> pathList = new ArrayList<>();
        //         for (String string : Arrays.asList(path.split("/"))) {
        //             if (string.contains("=")) {
        //                 continue;
        //             }
        //             pathList.add(string);
        //         }
        //         targetTableName = pathList.get(pathList.size() - 1);
        //     } else {
        //         targetTableName = path.substring(path.lastIndexOf("/") + 1);
        //     }
        // } else {
        //     targetTableName = writerConfigParameter.getConnection().get(0).getTable().get(0);
        // }
        targetTableName = "tb_student_zwd_test_myrepo";
        return targetTableName;
    }

    /**
     * 新增任务执行记录
     * @param confProperties
     */
    // @SuppressWarnings("unchecked")
    public static Map<String, Object> addExecRecordBeforeExec(Properties confProperties,
                                               String jobIdString,
                                               DataTransferConfig config) throws IOException {
        Map<String, Object> paramResMap = new HashMap<String, Object>();

        String execDate = confProperties.getProperty("execDate");
        Map dbConnectMap = JsonUtils.jsonStrToObject(confProperties.getProperty("dbConnect"), Map.class);
        Map<String, Object> paramMap = TaskExec.addTbDagRun(jobIdString, execDate, dbConnectMap);

        JobConfig jobConfig = config.getJob();
        String sourceTableName = null;
        String targetTableName = null;
        List convertList = new ArrayList();
        List<ContentConfig> contentConfigList = jobConfig.getContent();
        List<Integer> indexList = new ArrayList<>();
        String readerName = null;
        List<String> columnList = null;
        for (ContentConfig contentConfig : contentConfigList) {
            ReaderConfig readerConfig = contentConfig.getReader();
            ReaderConfig.ParameterConfig readerConfigParameter = readerConfig.getParameter();
            List<ReaderConfig.ParameterConfig.ConnectionConfig> readerConnectionList = readerConfigParameter.getConnection();
            logger.info("===Connection " + readerConnectionList);
            logger.info("===column " + readerConfigParameter.getColumn());
            // convertList = readerConfigParameter.getConver();
            logger.info("===conver " + convertList);
            readerName = readerConfig.getName();
            logger.info("===readerName {}", readerName);

            //writer信息
            WriterConfig writerConfig = contentConfig.getWriter();
            String writerName = writerConfig.getName();
            logger.info("===writerName {}", writerName);
            WriterConfig.ParameterConfig writerConfigParameter = writerConfig.getParameter();
            //hdfs writer需要特殊处理 ：根据hdfs路径获取表名，如果带有分区则需要特殊处理
            targetTableName = TaskExec.getTargetTableName(writerName, writerConfigParameter);
            //二次开发新增：hdfswriter的path加分区后，通过Connection拿到Table
            //targetTableName = writerConfigParameter.getConnection().get(0).getTable().get(0);
            logger.info("===targetTableName " + targetTableName);

            columnList = readerConfigParameter.getColumn();

            // //新架构
            // if (null == convertList || convertList.isEmpty()) {
            //     convertList = contentConfig.getConverter().getConvertList();
            //     logger.info("新架构 " + convertList);
            // }

            if ("kafka11reader".equalsIgnoreCase(readerName)) {
                // sourceTableName = readerConfigParameter.getTopic();
                confProperties.put("flink.checkpoint.interval", "10000");
                confProperties.put("flink.checkpoint.stateBackend", "file:///tmp/flinkx_checkpoint");

                continue;
            }

            if ("ftpreader".equalsIgnoreCase(readerName)) {
                continue;
            }
            if ("mongodbreader".equalsIgnoreCase(readerName)) {

                // sourceTableName = readerConfigParameter.getCollectionName();
                continue;
            }
            if ("hdfsreader".equalsIgnoreCase(readerName)) {
                // String path = readerConfigParameter.getPath();
                // logger.info("===path {}", path);
                // sourceTableName = path.substring(path.lastIndexOf("/") + 1);
                // List column = readerConfigParameter.getColumn();
                // for (Object o : column) {
                //     Map<String, Object> columnMap = (Map<String, Object>) o;
                //     indexList.add(((Double) columnMap.get("index")).intValue());
                // }
                // continue;
            } else {
                // sourceTableName = readerConnectionList.get(0).getTable().get(0);
                sourceTableName = "tb_student_zwd_test_myrepo";
            }
            // String sourceTables = readerConfigParameter.getSourceTables();
            // logger.info("===sourceTables" + sourceTables);
            // if (org.apache.commons.lang.StringUtils.isNotEmpty(sourceTables)) {
            //     sourceTableName = sourceTables;
            // }
            // logger.info("===sourceTableName" + sourceTableName);

        }
        paramResMap.put("convertList", convertList);
        paramResMap.put("indexList", indexList);
        paramResMap.put("columnList", columnList);
        paramResMap.put("paramMap", paramMap);
        paramResMap.put("sourceTableName", sourceTableName);
        paramResMap.put("targetTableName", targetTableName);
        paramResMap.put("dbConnectMap", dbConnectMap);
        return paramResMap;
    }

    /**
     * 修改任务执行记录
     */
    public static void addExecRecordAfterExec(String mode, Map<String, Object> paramMap,
                                              String sourceTableName,
                                              String targetTableName, long startTime,
                                              Map dbConnectMap, Properties confProperties) throws IOException {

        if ("local".equalsIgnoreCase(mode)) {
            TaskExec.updateTbDagRun(paramMap, sourceTableName, targetTableName, startTime, dbConnectMap);
            String isAll = confProperties.getProperty("isAll");
            if (isAll.equals("1")) {
                String colId = confProperties.getProperty("metaId");
                String maxValue = confProperties.getProperty("maxValue");
                TaskExec.updateTbMetadata(colId, maxValue, dbConnectMap);
            }
            return;
        }
        if ("yarn".equalsIgnoreCase(mode)) {
            logger.info("==yarn==");
        }
    }


}
