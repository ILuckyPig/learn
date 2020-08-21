package com.lu.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateUtil {
    /**
     * 毫秒时间戳转 {@link LocalDateTime}
     *
     * @param timestamp
     * @return
     */
    public static LocalDateTime trans2LocalDateTime(long timestamp) {
        return new Date(timestamp).toInstant().atZone(ZoneId.of("+8")).toLocalDateTime();
    }

    /**
     * 格式化 {@link LocalDateTime} 输出
     * @param localDateTime
     * @return
     */
    public static String format(LocalDateTime localDateTime) {
        return localDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    /**
     * 毫秒时间戳转 {@link LocalDateTime} 并格式化输出
     * @param timestamp
     * @return
     */
    public static String transFormat(long timestamp) {
        return format(trans2LocalDateTime(timestamp));
    }
}
