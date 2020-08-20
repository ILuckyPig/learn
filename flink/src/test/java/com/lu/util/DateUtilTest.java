package com.lu.util;

import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.junit.Assert.assertEquals;

public class DateUtilTest {
    @Test
    public void testTrans2LocalDateTime() {
        LocalDateTime time = DateUtil.trans2LocalDateTime(1597923420000L);
        LocalDateTime date = LocalDateTime.parse("2020-08-20T19:37:00");
        System.out.println(time.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        assertEquals(date, time);
    }
}
