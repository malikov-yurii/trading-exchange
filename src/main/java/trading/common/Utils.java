package trading.common;

import org.apache.commons.lang3.ObjectUtils;

public class Utils {
    public static String env(String envVar, String defaultValue) {
        return ObjectUtils.defaultIfNull(System.getenv(envVar), defaultValue);
    }
}
