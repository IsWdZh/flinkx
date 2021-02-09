package com.dtstack.flinkx.convert.component;

import com.dtstack.flinkx.convert.entity.OptEnum;
import com.dtstack.flinkx.convert.utils.DateFormateUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 日期格式转换组件工具类
 *
 * @author wangcaiwen
 * @date 2019/12/25 10:04
 */
public class DateFormatConvert implements Serializable {
    public static Logger LOG = LoggerFactory.getLogger(DateFormatConvert.class);

    public static DataStream<Row> doDateFormat(DataStream<Row> dataStream, List optList) {
        if (null != optList && !optList.isEmpty()) {
            for (Object optObject : optList) {
                Map<String, Object> optMap = (Map<String, Object>) optObject;
                Integer position = Integer.valueOf((String) optMap.get("position"));
                String opt = (String) optMap.get("opt");
                String format = (String) optMap.get("format");
                if (OptEnum.dateformat.toString().equalsIgnoreCase(opt)) {
                    dataStream = dataStream.map(new MapFunction<Row, Row>() {

                        @Override
                        public Row map(Row row) throws Exception {
//                            LOG.info("row is:" + row);
                            Object objectValue = row.getField(position - 1);
                            String stringValue = String.valueOf(objectValue);
//                            LOG.info("stringValue is:" + stringValue);
                            String formatown = DateFormatConvert.getformat(stringValue);
//                            LOG.info("formatown is:" + formatown);
                            if (null != formatown) {
                                DateFormat dateFormat = new SimpleDateFormat(formatown);
                                Date dateValue = dateFormat.parse(stringValue);
                                DateFormat dateFormatNew = new SimpleDateFormat(format);
                                String dateValueNew = dateFormatNew.format(dateValue);
                                row.setField(position - 1, dateValueNew);
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
     * 字符串解析 得到其对应的format
     *
     * @param stringValue
     * @return String formatown
     */
    private static String getformat(String stringValue) {
        String[] formateRegulars = DateFormateUtil.createRegulars();
        String formatown = null;
        //比对正则库，获取formateown
        for (String formateregular : formateRegulars) {
            if (stringValue.matches(formateregular)) {
                Map<String, String> Map = DateFormateUtil.dateFormatMap();
                for (String getKey : Map.keySet()) {
                    if (Map.get(getKey).equals(formateregular)) {
                        formatown = getKey;
                        break;
                    }
                }
            }
        }
        if (null == formatown) {
            LOG.info("解析失败");
        }
        return formatown;
    }
}
