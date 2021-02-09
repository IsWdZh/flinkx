package com.dtstack.flinkx.convert.component;

import com.dtstack.flinkx.convert.entity.OptEnum;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.MIN_VALUE;

/**
 * 按某个字段值过滤工具类
 *
 * @author liuwenjie
 * @date 2019/11/11 9:07
 */
public class FilterConvert implements Serializable {

    public static Logger LOG = LoggerFactory.getLogger(FilterConvert.class);

    public static DataStream<Row> doFilter(DataStream<Row> dataStream, List optList,List<Integer> indexList,List<String> columnList) {
        if (null != optList && !optList.isEmpty()) {
            Integer missFiledNum=0;
            for (Object optObject : optList) {
                Map<String, Object> optMap = (Map<String, Object>) optObject;
                //更新position ，减missFiledNum
                String name = (String) optMap.get("name");
                Integer originPosition = Integer.valueOf((String) optMap.get("position"));
                Integer position = originPosition - missFiledNum;
                if(indexList.isEmpty()){
                    missFiledNum = originPosition - SplitConvert.getRelativePos(name, columnList);
                    position=originPosition - missFiledNum;
                }else{
                    missFiledNum=0;
                    position=indexList.indexOf(originPosition)+1;
                }
                String opt = (String) optMap.get("opt");
                //换switch
                String strValue = String.valueOf(optMap.get("value"));
                Integer value;
                if (opt.equalsIgnoreCase(OptEnum.lt.toString())) {
                    value = strValue.trim().length() < 1 ? MAX_VALUE : Integer.valueOf(strValue);
                    Integer finalPosition = position;
                    dataStream = dataStream.filter(new FilterFunction<Row>() {
                        @Override
                        public boolean filter(Row row) throws Exception {
                            Object objectValue = row.getField(finalPosition - 1);
                            //数据表中字段值为空时的情况
                            Integer integerValue;
                            if ((objectValue == null) || ((String.valueOf(objectValue).trim().length() < 1))) {
                                integerValue = MAX_VALUE;
                            } else {
                                String stringValue = String.valueOf(objectValue);
                                integerValue = Integer.valueOf(stringValue);
                            }

                            return integerValue < value ? true : false;
                        }
                    });
                } else if (opt.equalsIgnoreCase(OptEnum.gt.toString())) {
                    value = strValue.trim().length() < 1 ? MIN_VALUE : Integer.valueOf(strValue);
                    Integer finalPosition = position;
                    dataStream = dataStream.filter(new FilterFunction<Row>() {
                        @Override
                        public boolean filter(Row row) throws Exception {
                            Object objectValue = row.getField(finalPosition - 1);
                            //数据表中字段值为空时的情况
                            Integer integerValue;
                            if ((objectValue == null) || ((String.valueOf(objectValue).trim().length() < 1))) {
                                integerValue = MIN_VALUE;
                            } else {
                                String stringValue = String.valueOf(objectValue);
                                integerValue = Integer.valueOf(stringValue);
                            }
                            return integerValue > value ? true : false;
                        }
                    });
                } else if (opt.equalsIgnoreCase(OptEnum.eq.toString())) {
                    value = strValue.trim().length() < 1 ? MIN_VALUE : Integer.valueOf(strValue);
                    Integer finalPosition = position;
                    dataStream = dataStream.filter(new FilterFunction<Row>() {
                        @Override
                        public boolean filter(Row row) throws Exception {
                            //row:2,aa,12,1990-01-05,2
                            LOG.info("filter eq row:" + row.toString());
                            Object objectValue = row.getField(finalPosition - 1);
                            //数据表中字段值为空时的情况
                            Integer integerValue;
                            if ((objectValue == null) || ((String.valueOf(objectValue).trim().length() < 1))) {
                                integerValue = MIN_VALUE;
                            } else {
                                String stringValue = String.valueOf(objectValue);
                                integerValue = Integer.valueOf(stringValue);
                            }
                            return integerValue.equals(value) ? true : false;
                        }
                    });
                } else if (opt.equalsIgnoreCase(OptEnum.lte.toString())) {
                    value = strValue.trim().length() < 1 ? MAX_VALUE : Integer.valueOf(strValue);
                    Integer finalPosition = position;
                    dataStream = dataStream.filter(new FilterFunction<Row>() {
                        @Override
                        public boolean filter(Row row) throws Exception {
                            //row:2,aa,12,1990-01-05,2
                            LOG.info("filter lte row:" + row.toString());
                            Object objectValue = row.getField(finalPosition - 1);
                            //数据表中字段值为空时的情况
                            Integer integerValue;
                            if ((objectValue == null) || ((String.valueOf(objectValue).trim().length() < 1))) {
                                integerValue = MAX_VALUE;
                            } else {
                                String stringValue = String.valueOf(objectValue);
                                integerValue = Integer.valueOf(stringValue);
                            }
                            return integerValue <= value ? true : false;
                        }
                    });
                } else if (opt.equalsIgnoreCase(OptEnum.gte.toString())) {
                    value = strValue.trim().length() < 1 ? MIN_VALUE : Integer.valueOf(strValue);
                    Integer finalPosition = position;
                    dataStream = dataStream.filter(new FilterFunction<Row>() {
                        @Override
                        public boolean filter(Row row) throws Exception {
                            Object objectValue = row.getField(finalPosition - 1);

                            //数据表中字段值为空时的情况
                            Integer integerValue;
                            if ((objectValue == null) || ((String.valueOf(objectValue).trim().length() < 1))) {
                                integerValue = MIN_VALUE;
                            } else {
                                String stringValue = String.valueOf(objectValue);
                                integerValue = Integer.valueOf(stringValue);
                            }
                            return integerValue >= value ? true : false;
                        }
                    });
                }
            }
        }
        return dataStream;
    }
}
