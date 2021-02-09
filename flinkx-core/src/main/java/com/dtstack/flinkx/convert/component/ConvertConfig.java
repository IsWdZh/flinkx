package com.dtstack.flinkx.convert.component;

import com.dtstack.flinkx.config.AbstractConfig;

import java.util.List;
import java.util.Map;

/**
 * @author liuwenjie
 * @date 2019/12/12 15:41
 */
public class ConvertConfig extends AbstractConfig {

    List convertList;

    public static String KEY_CONVERTER_CONFIG = "converter";

    public ConvertConfig(Map<String, Object> map) {
        super(map);
    }

    public List getConvertList() {
        return convertList;
    }

}
