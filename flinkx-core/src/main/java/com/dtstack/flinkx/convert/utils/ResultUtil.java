package com.dtstack.flinkx.convert.utils;

import com.dtstack.flinkx.util.ResultPrintUtil;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.JobExecutionResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: Zhou Wendi
 * @Date: 2020/12/11
 */
public class ResultUtil extends ResultPrintUtil {
    public final static Map<String,Object> taskDetailMap = new HashMap<>();

    public static Map<String, Object> getTaskDetailMap() {
        return taskDetailMap;
    }

    public static void makeTaskDetailMap(JobExecutionResult result) {
        List<String> names = Lists.newArrayList();
        List<String> values = Lists.newArrayList();
        result.getAllAccumulatorResults().forEach((name, val) -> {
            names.add(name);
            values.add(String.valueOf(val));
        });

        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);

            if("numRead".equalsIgnoreCase(name)){
                taskDetailMap.put("numRead",values.get(i));
            }
            if("numWrite".equalsIgnoreCase(name)){
                taskDetailMap.put("numWrite",values.get(i));
            }
            if("nErrors".equalsIgnoreCase(name)){
                taskDetailMap.put("nErrors",values.get(i));
            }
            if("byteRead".equalsIgnoreCase(name)){
                taskDetailMap.put("byteRead",values.get(i));
            }
            if("byteWrite".equalsIgnoreCase(name)){
                taskDetailMap.put("byteWrite",values.get(i));
            }
        }
    }

}
