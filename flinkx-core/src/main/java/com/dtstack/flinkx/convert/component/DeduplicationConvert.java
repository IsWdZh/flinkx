package com.dtstack.flinkx.convert.component;

import com.dtstack.flinkx.convert.entity.OptEnum;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class DeduplicationConvert {
    public static Logger LOG = LoggerFactory.getLogger(DeduplicationConvert.class);

    public static DataStream doDeduplDication(DataStream<Row> dataStream, List optList,List<Integer> indexList,List<String> columnList) throws Exception {
        if (null != optList && !optList.isEmpty()) {
            for (Object optObject : optList) {
                Map<String, Object> optMap = (Map<String, Object>) optObject;
                List columnsList = (List) optMap.get("columns");
                String opt = (String) optMap.get("opt");
                if (opt.equalsIgnoreCase(OptEnum.deduplication.toString())) {
                    BloomFilter bf = new BloomFilter();
                    dataStream = dataStream.filter(new FilterFunction<Row>() {
                        @Override
                        public boolean filter(Row row) throws Exception {
                            //LOG.info("filter lt row:" + row.toString());
                            //拿到用@|.|@连接的指定去重字段的拼接字符串
                            String columnsStr = "+";
                            for (Object columnsObject : columnsList) {
                                Integer missFiledNum=0;
                                Map<String, Object> columnsMap = (Map<String, Object>) columnsObject;
                                //更新position ，减missFiledNum
                                String name = (String) columnsMap.get("name");
                                Integer originPosition = Integer.valueOf((String) columnsMap.get("position"));
                                Integer position = originPosition - missFiledNum;
                                if(indexList.isEmpty()){
                                    missFiledNum = originPosition - SplitConvert.getRelativePos(name, columnList);
                                    position=originPosition - missFiledNum;
                                }else{
                                    missFiledNum=0;
                                    position=indexList.indexOf(originPosition)+1;
                                }
                                Object objectValue = row.getField(position - 1);
                                String stringValue = (objectValue == null) ? "@|.|@" : objectValue.toString();
                                columnsStr = columnsStr + "@|.|@" + stringValue;
                            }
                            if (bf.contains(columnsStr)) {
                                return false;
                            } else {
                                bf.add(columnsStr);
                                return true;
                            }
                        }
                    });
                }
            }
        }
        return dataStream;
    }
}


