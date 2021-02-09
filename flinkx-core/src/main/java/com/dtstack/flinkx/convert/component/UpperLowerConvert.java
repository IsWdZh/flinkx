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
 * 大小写转换组件工具类
 *
 * @author liuwenjie
 * @date 2019/10/21 10:44
 */
public class UpperLowerConvert implements Serializable {

    public static Logger LOG = LoggerFactory.getLogger(UpperLowerConvert.class);

    public static DataStream<Row> doUpperLower(DataStream<Row> dataStream, List optList,List<Integer> indexList,List<String> columnList) {
        if (null != optList && !optList.isEmpty()) {
            Integer missFiledNum=0;
            for (Object optObject : optList) {
                Map<String, Object> optMap = (Map<String, Object>) optObject;
                //更新position ，减missFiledNum
                String name = (String) optMap.get("name");
                Integer originPosition = Integer.valueOf((String) optMap.get("position"));
                Integer position = originPosition - missFiledNum;
                if(indexList.isEmpty()){
                    //获取相对位置
                    missFiledNum = originPosition - SplitConvert.getRelativePos(name, columnList);
                    position=originPosition - missFiledNum;
                }else{
                    missFiledNum=0;
                    position=indexList.indexOf(originPosition)+1;
                }

                String opt = (String) optMap.get("opt");
                if (OptEnum.upper.toString().equalsIgnoreCase(opt)) {
                    Integer finalPosition1 = position;
                    dataStream = dataStream.map(new MapFunction<Row, Row>() {
                        @Override
                        public Row map(Row row) throws Exception {
                            //row:2,aa,12,1990-01-05,2
                            //LOG.info("upper row:" + row.toString());
                            Object fieldObject = row.getField(finalPosition1 - 1);
                            if (!(fieldObject == null)) {
                                String strvalue = String.valueOf(fieldObject);
                                row.setField(finalPosition1 - 1, strvalue.toUpperCase());
                            }

                            return row;
                        }
                    });
                } else if (OptEnum.lower.toString().equalsIgnoreCase(opt)) {
                    Integer finalPosition = position;
                    dataStream = dataStream.map(new MapFunction<Row, Row>() {
                        @Override
                        public Row map(Row row) throws Exception {
                            //row:2,aa,12,1990-01-05,2
                            //LOG.info("lower row:" + row.toString());
                            Object fieldObject = row.getField(finalPosition - 1);
                            if (!(fieldObject == null)) {
                                String strvalue = String.valueOf(fieldObject);
                                row.setField(finalPosition - 1, strvalue.toLowerCase());
                            }
                            return row;
                        }
                    });
                }
            }
        }
        return dataStream;
    }
}
