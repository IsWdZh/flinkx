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
 * @author wangcaiwen
 * @date 2020/01/15 14:16
 * @description
 */
public class SplitConvert implements Serializable {
    public static Logger LOG = LoggerFactory.getLogger(SplitConvert.class);
    public static DataStream<Row> doSplit(DataStream<Row> dataStream, List optList,List<Integer> indexList,List<String> columnList) {

        if (null != optList && !optList.isEmpty()) {
            Integer addPosionNum = 0;
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
//                Map<String, Object> splitRuleMap = (Map<String, Object>) optObject;
                List splitRuleList = (List) optMap.get("split_rules");
                if (OptEnum.split.toString().equalsIgnoreCase(opt)) {
                    Integer finalPosition = position;
                    dataStream = dataStream.map(new MapFunction<Row, Row>() {

                        @Override
                        public Row map(Row row) throws Exception {
                            //提取Row的String
                            //LOG.info("splitString row:" + row.toString());
                            Object object = row.getField(finalPosition - 1);
                            String Str;
//                            String Str= object == null?"":String.valueOf(object);
                            //新建newRow
                            Row newRow = new Row(splitRuleList.size() + row.getArity() - 1);
                            //得到拆分后的str//遍历splitRuleList
                            String[] subStrs = new String[splitRuleList.size()];
                            for (int i = 0; i < splitRuleList.size(); i++) {
                                Map<String, Object> splitMap = (Map<String, Object>) splitRuleList.get(i);
                                String subName = (String) splitMap.get("subName");
                                String strStart = String.valueOf(splitMap.get("start"));
                                Integer start = strStart.trim().length() < 1 ? MIN_VALUE : Integer.valueOf(strStart);
                                String strEnd = String.valueOf(splitMap.get("end"));
                                Integer end = strEnd.trim().length() < 1 ? MAX_VALUE : Integer.valueOf(strEnd);
                                if (!(object == null)) {
                                    Str = String.valueOf(object);
                                    subStrs[i] = substring(start, end, Str);
                                } else {
                                    subStrs[i] = substring(start, end, null);
                                }
                            }
                            //复制 （Row前，拆分后的str，Row后） 到newRow
                            for (int i = 0; i < newRow.getArity(); i++) {
                                if (i < finalPosition - 1) {
                                    newRow.setField(i, row.getField(i));
                                } else if (i < finalPosition + splitRuleList.size() - 1) {
                                    newRow.setField(i, subStrs[i - finalPosition + 1]);
                                } else {
                                    newRow.setField(i, row.getField(i - splitRuleList.size() + 1));
                                }

                            }
                            return newRow;
                        }

                    });
                }
                addPosionNum = addPosionNum + splitRuleList.size() - 1;
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

    static Integer getRelativePos(String filedName, List<String> list) {
        Integer index = 0;
        if (list.contains(filedName)) {
            index = list.indexOf(filedName) + 1;
        }
        return index;
    }
}

