package org.arpit.spark.common.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import org.arpit.spark.common.pojo.Employee;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class EmployeeUtil {

    private static transient ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static Employee fromJson(String employeeJson) {
        try {
            return OBJECT_MAPPER.readValue(employeeJson, Employee.class);
        } catch (IOException e) {
            e.printStackTrace();
            return new Employee();
        }
    }

    public static String buildRandomEmployeeJson() {
        try {
            return OBJECT_MAPPER.writeValueAsString(buildRandomEmployees(1).get(0));
        } catch (JsonProcessingException e) {
            return "{}";
        }
    }

    public static Employee buildRandomEmployee() {
        return buildRandomEmployees(1).get(0);
    }

    public static List<Employee> buildRandomEmployees(int size) {
        final Faker faker = new Faker(new Locale("en", "IND"));
        final List<Employee> employees = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            Employee employee = new Employee();
            employee.setId(Long.valueOf(i + 1));
            employee.setFirstName(faker.name().firstName());
            employee.setLastName(faker.name().lastName());
            employee.setGender(faker.demographic().sex());
            employee.setDoj(faker.date().past(3650, TimeUnit.DAYS));
            employee.setAge(faker.number().numberBetween(18, 65));
            employee.setEmail(faker.internet().emailAddress());
            employee.setMobile(faker.phoneNumber().cellPhone());
            employee.setSalary(faker.number().randomDouble(2, 60000, 200000));
            employee.setCompanyName(faker.company().name().replaceAll(",", ""));
            employee.setJobTitle(faker.job().title());
            employee.setCity(faker.address().city());
            employee.setCountry("India");
            employees.add(employee);
        }
        return employees;
    }
}
