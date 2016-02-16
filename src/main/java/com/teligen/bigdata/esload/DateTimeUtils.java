package com.teligen.bigdata.esload;

import org.apache.commons.lang.math.NumberUtils;
import org.elasticsearch.common.unit.TimeValue;

/**
 * Created by root on 2015/6/24.
 */
public class DateTimeUtils {
    public static long parseHumanDTToMills(String humanDateTime) {
        long datetime = 0;
        if (humanDateTime.indexOf("ms") != -1) {
            datetime = TimeValue.timeValueMillis(NumberUtils.toLong(humanDateTime.replaceAll("ms", ""))).getMillis();
        } else if (humanDateTime.indexOf("h") != -1) {
            datetime = TimeValue.timeValueHours(NumberUtils.toLong(humanDateTime.replaceAll("h", ""))).getMillis();
        } else if (humanDateTime.indexOf("m") != -1) {
            datetime = TimeValue.timeValueMinutes(NumberUtils.toLong(humanDateTime.replaceAll("m", ""))).getMillis();
        } else if (humanDateTime.indexOf("s") != -1) {
            datetime = TimeValue.timeValueSeconds(NumberUtils.toLong(humanDateTime.replaceAll("s", ""))).getMillis();
        }
        return datetime;
    }

    public static void main(String[] args) {
        System.out.println(parseHumanDTToMills("1h"));
        System.out.println(parseHumanDTToMills("1m"));
        System.out.println(parseHumanDTToMills("2s"));
        System.out.println(parseHumanDTToMills("2ms"));
    }
}
