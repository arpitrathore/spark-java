package org.arpit.spark.common.util;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.List;

public class CsvUtility {
    private final static char DEFAULT_SEPARATOR = ' ';

    public static void main(String[] args) {
        System.out.println(buildCsvString(EmployeeUtil.buildRandomEmployees(3), ",", true));
    }

    public static String buildCsvString(List<?> objectList, String separator, boolean displayHeader) {
        StringBuilder result = new StringBuilder();
        if (objectList.size() == 0) {
            return result.toString();
        }
        if (displayHeader) {
            result.append(getHeaders(objectList.get(0), separator));
            result.append("\n");
        }
        for (Object obj : objectList) {
            result.append(addObjectRow(obj, separator)).append("\n");
        }
        return result.toString();
    }

    public static String getHeaders(Object obj, String separator) {
        StringBuilder resultHeader = new StringBuilder();
        boolean firstField = true;
        Field fields[] = obj.getClass().getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            String value;
            try {
                value = field.getName();
                if (firstField) {
                    resultHeader.append(value);
                    firstField = false;
                } else {
                    resultHeader.append(separator).append(value);
                }
                field.setAccessible(false);
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            }
        }
        return resultHeader.toString();
    }


    public static String addObjectRow(Object obj, String separator) {
        StringBuilder csvRow = new StringBuilder();
        Field fields[] = obj.getClass().getDeclaredFields();
        boolean firstField = true;
        for (Field field : fields) {
            field.setAccessible(true);
            Object value;
            try {
                value = field.get(obj);
                if (field.getType().equals(Date.class)) {
                    value = ((Date) value).getTime();
                }
                if (value == null)
                    value = "";
                if (firstField) {
                    csvRow.append(value);
                    firstField = false;
                } else {
                    csvRow.append(separator).append(value);
                }
                field.setAccessible(false);
            } catch (IllegalArgumentException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return csvRow.toString();
    }
}
