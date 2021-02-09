package com.dtstack.flinkx.convert;

import com.dtstack.flinkx.convert.component.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

/**
 * @Author: Zhou Wendi
 * @Date: 2020/12/11
 */
public class Convert {
    public static DataStream<Row> convert(DataStream<Row> dataStream, List convertList, List<Integer> indexList, List<String> columnList) {
        if (null != convertList && !convertList.isEmpty()) {
            for (Object convert : convertList) {
                Map<String, Object> convertMap = (Map<String, Object>) convert;
                //1.大小写转换组件
                Object upperLowerConvertObject = convertMap.get("upper_lower_convert");
                if (null != upperLowerConvertObject) {
                    List optList = (List) upperLowerConvertObject;
                    dataStream = UpperLowerConvert.doUpperLower(dataStream, optList, indexList, columnList);
                }
                //2.字符串截取组件
                Object substringConvertObject = convertMap.get("substring_convert");
                if (null != substringConvertObject) {
                    List optList = (List) substringConvertObject;
                    dataStream = SubstringConvert.doSubstring(dataStream, optList, indexList, columnList);
                }
                //3.按字段数值过滤组件
                Object filterConvertObject = convertMap.get("filter_convert");
                if (null != filterConvertObject) {
                    List optList = (List) filterConvertObject;
                    dataStream = FilterConvert.doFilter(dataStream, optList, indexList, columnList);
                }
                //4.字段替换转换组件
                Object valueMappingConvertObject = convertMap.get("value_mapping_convert");
                if (null != valueMappingConvertObject) {
                    List optList = (List) valueMappingConvertObject;
                    dataStream = ValueMappingConvert.doValueMapping(dataStream, optList, indexList, columnList);
                }
                //5.日期格式转换组件
                Object dateFormateConvertObject = convertMap.get("date_format_convert");
                if (null != dateFormateConvertObject) {
                    List optList = (List) dateFormateConvertObject;
                    dataStream = DateFormatConvert.doDateFormat(dataStream, optList);
                }
                //6.字段去重转换组件
                Object deduplicationConvertObject = convertMap.get("deduplication_convert");
                if (null != deduplicationConvertObject) {
                    List optList = (List) deduplicationConvertObject;
                    try {
                        dataStream = DeduplicationConvert.doDeduplDication(dataStream, optList, indexList, columnList);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                //7.字段补齐转换组件
                Object dataCompletionConvertObject = convertMap.get("data_completion_convert");
                if (null != dataCompletionConvertObject) {
                    List optList = (List) dataCompletionConvertObject;
                    dataStream = DataCompletionConvert.doDataCompletion(dataStream, optList, indexList, columnList);
                }

                //8.字段加密组件
                Object encryptionConvertObject = convertMap.get("encryption_convert");
                if (null != encryptionConvertObject) {
                    List optList = (List) encryptionConvertObject;
                    dataStream = EncryptionConvert.doEncryption(dataStream, optList, indexList, columnList);
                }

            }
            for (Object convert : convertList) {
                Map<String, Object> convertMap = (Map<String, Object>) convert;
                //7.字段拆分转换组件
                Object splitConvertObject = convertMap.get("split_convert");
                if (null != splitConvertObject) {
                    List optList = (List) splitConvertObject;
                    dataStream = SplitConvert.doSplit(dataStream, optList, indexList, columnList);
                }
            }
        }
        return dataStream;
    }



}
