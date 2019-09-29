package com.lu.utils;

import org.yaml.snakeyaml.Yaml;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class YamlUtils {

    private static Yaml yaml = new Yaml();
    private static List<String> list = (List<String>) yaml.load(YamlUtils.class.getResourceAsStream("/application.yaml"));

    public static Map<Integer, String> getUserMap() {
        Map<Integer, String> userMap = new HashMap<>();
        for (String user : list) {
            int uid = Integer.parseInt(user.split(",")[0]);
            userMap.put(uid, user);
        }
        return userMap;
    }
}
