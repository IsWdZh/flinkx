package com.dtstack.flinkx.convert.component;

import com.dtstack.flinkx.convert.entity.OptEnum;
import org.apache.flink.api.common.functions.MapFunction;
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
 * 字符串截取工具类
 *
 * @author liuwenjie
 * @date 2019/11/11 9:05
 */
public class SubstringConvert implements Serializable {
    public static Logger LOG = LoggerFactory.getLogger(SubstringConvert.class);

    public static DataStream<Row> doSubstring(DataStream<Row> dataStream, List optList,List<Integer> indexList,List<String> columnList) {
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
//
                String strStart = String.valueOf(optMap.get("start"));
                Integer start = strStart.trim().length() < 1 ? MIN_VALUE : Integer.valueOf(strStart);
                String strEnd = String.valueOf(optMap.get("end"));
                Integer end = strEnd.trim().length() < 1 ? MAX_VALUE : Integer.valueOf(strEnd);
                if (OptEnum.substring.toString().equalsIgnoreCase(opt)) {
                    Integer finalPosition = position;
                    dataStream = dataStream.map(new MapFunction<Row, Row>() {
                        @Override
                        public Row map(Row row) throws Exception {
                            //row:2,aa,12,1990-01-05,2
                            //LOG.info("substring row:" + row.toString());
                            Object object = row.getField(finalPosition - 1);
                            if (!(object == null)) {
//                                String str= object == null?"":String.valueOf(object);
                                String str = String.valueOf(object);
                                //str=str.substring(start,end);
                                str = substring(start, end, str);
                                row.setField(finalPosition - 1, str);
                            }
                            return row;
                        }
                    });
                }
            }
        }
        return dataStream;
    }

    /**
     * 字符串截取处理方法
     *
     * @param start
     * @param end
     * @param str
     * @return
     */
    private static String substring(int start, int end, String str) {
        if (null != str && !str.isEmpty() && end >= start) {
            int length = str.length();
            //1.start end 都在正常范围的情况
            if (start > -1 && start < length && end > -1 && end < length) {
                String substring = str.substring(start, end);
                return substring;
            }
            //2.start 在正常范围的情况，end不在
            if (start > -1 && start < length && (end < 0 || end >= length)) {
                String substring = str.substring(start, length);
                return substring;
            }
            //3.start 不在正常范围的情况，end在
            if ((start < 0 || start >= length) && end > -1 && end < length) {
                String substring = str.substring(0, end);
                return substring;
            }
            //4.其他情况不做处理原样子返回
        }
        return str;
    }
}
