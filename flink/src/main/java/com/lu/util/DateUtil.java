package com.lu.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
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
}
