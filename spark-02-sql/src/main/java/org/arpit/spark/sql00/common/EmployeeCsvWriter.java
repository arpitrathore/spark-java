package org.arpit.spark.sql00.common;

import com.google.common.io.Files;
import org.arpit.spark.common.pojo.Employee;
import org.arpit.spark.common.util.CsvUtility;
import org.arpit.spark.common.util.EmployeeUtil;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Builds employee csv and writes it to src/main/resources
 */
public class EmployeeCsvWriter {

    private static final int ROW_COUNT = 100000;
    private static String EMPLOYEE_CSV_FILE_PATH = "spark-02-sql/src/main/resources/employees.csv";

    public static void main(String[] args) throws IOException {
        File destinationFile = new File(EMPLOYEE_CSV_FILE_PATH);
        if (destinationFile.exists()) {
            System.out.println("File already exist : " + EMPLOYEE_CSV_FILE_PATH);
            System.out.println("Delete the file and run again to re-create the csv file.");
            return;
        }

        System.out.println("Creating file at : " + EMPLOYEE_CSV_FILE_PATH);
        destinationFile.createNewFile();
        List<Employee> employees = EmployeeUtil.buildRandomEmployees(ROW_COUNT);
        byte[] employeeCsvBytes = CsvUtility.buildCsvString(employees, ",", true).getBytes();
        Files.write(employeeCsvBytes, destinationFile);
        System.out.println("File created successfully. Row count : " + ROW_COUNT);
    }


}
