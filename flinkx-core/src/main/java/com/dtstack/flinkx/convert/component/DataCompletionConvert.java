package com.dtstack.flinkx.convert.component;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 数据补齐组件
 *
 * @author liuwenjie
 * @date 2020/06/22 16:03
 */
public class DataCompletionConvert implements Serializable {

    public static Logger LOG = LoggerFactory.getLogger(DataCompletionConvert.class);

    public static DataStream<Row> doDataCompletion(DataStream<Row> dataStream, List optList, List<Integer> indexList, List<String> columnList) {
        if (null != optList && !optList.isEmpty()) {
            Integer missFiledNum = 0;
            for (Object optObject : optList) {
                Map<String, Object> optMap = (Map<String, Object>) optObject;
                Object value = optMap.get("value");
                //更新position ，减missFiledNum
                String name = (String) optMap.get("name");
                Integer originPosition = Integer.valueOf((String) optMap.get("position"));
                Integer position = originPosition - missFiledNum;
                if (indexList.isEmpty()) {
                    //获取相对位置
                    missFiledNum = originPosition - SplitConvert.getRelativePos(name, columnList);
                    position = originPosition - missFiledNum;
                } else {
                    missFiledNum = 0;
                    position = indexList.indexOf(originPosition) + 1;
                }
                Integer finalPosition1 = position;
                dataStream = dataStream.map(new MapFunction<Row, Row>() {
                    @Override
                    public Row map(Row row) throws Exception {
                        Object fieldObject = row.getField(finalPosition1 - 1);
                        if(null==fieldObject){
                            row.setField(finalPosition1 - 1, value);
                        }
                        return row;
                    }
                });
            }
        }
        return dataStream;
    }
}
