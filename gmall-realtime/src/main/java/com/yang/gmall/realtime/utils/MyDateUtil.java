package com.yang.gmall.realtime.utils;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class MyDateUtil {
    private static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static long toTs(String dateStr){
        LocalDateTime localDateTime = LocalDateTime.parse(dateStr, dtf);
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    public static String toYMDhms(Date date){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        String dateStr = dtf.format(localDateTime);
        return dateStr;
    }
    public static void main(String[] args) {
        String birthday = "2006-04-16";
        LocalDate nowTime = LocalDate.now();
        LocalDate birthtime = LocalDate.parse(birthday);
        Period between = Period.between(birthtime, nowTime);
        int age = between.getYears();
        //System.out.println(age);

        long start = 1619436774;
        System.out.println(toYMDhms(new Date(start)));
    }
}
