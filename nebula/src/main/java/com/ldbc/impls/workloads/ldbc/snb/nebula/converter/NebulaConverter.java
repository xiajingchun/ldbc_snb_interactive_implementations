package com.ldbc.impls.workloads.ldbc.snb.nebula.converter;

import com.ldbc.impls.workloads.ldbc.snb.converter.Converter;
import com.vesoft.nebula.DateTime;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class NebulaConverter extends Converter {

    public static String DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";
    final static String DATE_FORMAT = "yyyy-MM-dd";

    public static long convertDateTimesToEpoch(String date) throws ParseException {
        final SimpleDateFormat sdf = new SimpleDateFormat(DATETIME_FORMAT);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        return sdf.parse(date).toInstant().toEpochMilli();
    }

    public static long convertDateToEpoch(String date) throws ParseException {
        final SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        return sdf.parse(date).toInstant().toEpochMilli();
    }

    public static int convertStartAndEndDateToLatency(String from, String to) throws ParseException {
        long fromEpoch = convertDateTimesToEpoch(from);
        long toEpoch = convertDateTimesToEpoch(to);
        return (int)((toEpoch - fromEpoch) / 1000 / 60);
    }


    @Override
    public String convertDateTime(Date date) {
        final SimpleDateFormat sdf = new SimpleDateFormat(DATETIME_FORMAT);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        return "datetime('" + sdf.format(date) + "')";
    }

}
