package org.apache.beam.demos.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @Description
 * @Author liuzhiqiang
 * @Date 2019/7/19 14:02
 */
public class DateUtil {
    private static SimpleDateFormat getDateFormat(DateFormat df){
        return new SimpleDateFormat(df.getPattern());
    }

    public static Date parseDate(String dateStr, DateFormat format){
        try {
            return getDateFormat(format).parse(dateStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return Calendar.getInstance().getTime();
    }

    public static long signalDate2Timestamp(String dateStr){
        try {
            return getDateFormat(DateFormat.YYYYMMDDHHMMSS).parse(dateStr).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return Calendar.getInstance().getTime().getTime();
    }

}
