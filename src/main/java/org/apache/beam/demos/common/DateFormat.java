package org.apache.beam.demos.common;

/**
 * @Description
 * @Author liuzhiqiang
 * @Date 2019/7/19 14:15
 */
public enum DateFormat {
    YYYYMMDD("yyyy-MM-dd"),
    YYYYMMDDHHMM("yyyy-MM-dd HH:mm"),
    YYYYMMDDHHMMSS("yyyy-MM-dd HH:mm:ss"),
    YYYYMMDDHHMMSSSSS("yyyy-MM-dd HH:mm:ss SSS"),
    YYYYMMDDHHMM_SLASH("yyyy/MM/dd HH:mm"),
    YYYYMMDDHHMMSS_SLASH("yyyy/MM/dd HH:mm:ss");

    private String pattern;

    DateFormat(String pattern){
        this.pattern = pattern;
    }

    public String getPattern(){
        return pattern;
    }
}
