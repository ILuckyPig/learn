package com.lu.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class DateUtil {
    /**
     * 毫秒时间戳转 {@link LocalDateTime}
     *
     * @param timestamp
     * @return
     */
    public static LocalDateTime trans2LocalDateTime(long timestamp) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of("+8"));
    }
}
