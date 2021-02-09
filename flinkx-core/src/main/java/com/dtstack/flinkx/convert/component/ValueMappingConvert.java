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

/**
 * 值映射工具类
 *
 * @author wangcaiwen
 * @date 2019-12-23
 */
public class ValueMappingConvert implements Serializable {

    public static Logger LOG = LoggerFactory.getLogger(ValueMappingConvert.class);

    public static DataStream<Row> doValueMapping(DataStream<Row> dataStream, List optList,List<Integer> indexList,List<String> columnList) {
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
                List mappingRuleList = (List) optMap.get("mapping_rules");
                if (OptEnum.valuemapping.toString().equalsIgnoreCase(opt)) {
                    Integer finalPosition = position;
                    dataStream = dataStream.map(new MapFunction<Row, Row>() {
                        @Override
                        public Row map(Row row) throws Exception {
//                            LOG.info("row is:" + row);
                            //LOG.info("mappingString row:" + row.toString());
                            Object objectValue = row.getField(finalPosition - 1);
                            if (!(objectValue == null)) {
                                String stringValue = String.valueOf(objectValue);
                                for (int i = 0; i < mappingRuleList.size(); i++) {
                                    Map<String, Object> mappingMap = (Map<String, Object>) mappingRuleList.get(i);
                                    String value = (String) mappingMap.get("value");
                                    String mapped_value = (String) mappingMap.get("mapped_value");
                                    if (stringValue.equals(value)) {
                                        row.setField(finalPosition - 1, mapped_value);
                                    }
                                }
                            }
                            //LOG.info("after row:" + row.toString());
                            return row;
                        }
                    });

                }
            }
        }
        return dataStream;
    }
}
