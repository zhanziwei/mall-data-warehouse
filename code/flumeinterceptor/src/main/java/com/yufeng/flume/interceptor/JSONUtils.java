package com.yufeng.flume.interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;

public class JSONUtils {
    // 验证数据是否是JSON
    public static boolean isJSONValidate(String log) {
        try {
            JSON.parse(log);
            return true;
        } catch(JSONException e) {
            return false;
        }
    }
}
