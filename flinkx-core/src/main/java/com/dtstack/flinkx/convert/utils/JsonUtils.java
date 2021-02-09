package com.dtstack.flinkx.convert.utils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerationException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParseException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * @Author: Zhou Wendi
 * @Date: 2020/12/11
 */
public class JsonUtils {
    private static ObjectMapper objectMapper = new ObjectMapper();

    public static <T> T jsonStrToObject(String jsonStr, Class<T> clazz) throws JsonParseException, JsonMappingException, JsonGenerationException, IOException {
        return  objectMapper.readValue(jsonStr, clazz);
    }
}
