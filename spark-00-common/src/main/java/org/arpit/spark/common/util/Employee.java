package org.arpit.spark.common.util;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Employee implements Serializable {

    @JsonIgnore
    private static transient ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private String id;
    private String firstName;
    private String lastName;
    private String gender;
    private Date doj;
    private int age;
    private String email;
    private String mobile;
    private String companyName;
    private String jobTitle;
    private String city;
    private String country;

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
            employee.id = UUID.randomUUID().toString();
            employee.firstName = faker.name().firstName();
            employee.lastName = faker.name().lastName();
            employee.gender = faker.demographic().sex();
            employee.doj = faker.date().past(3650, TimeUnit.DAYS);
            //employee.doj = new Date();
            employee.age = faker.number().numberBetween(18, 65);
            employee.email = faker.internet().emailAddress();
            employee.mobile = faker.phoneNumber().cellPhone();
            employee.companyName = faker.company().name();
            employee.jobTitle = faker.job().title();
            employee.city = faker.address().city();
            employee.country = faker.address().country();
            employees.add(employee);
        }
        return employees;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public Date getDoj() {
        return doj;
    }

    public void setDoj(Date doj) {
        this.doj = doj;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getMobile() {
        return mobile;
    }

    public void setMobile(String mobile) {
        this.mobile = mobile;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public String getJobTitle() {
        return jobTitle;
    }

    public void setJobTitle(String jobTitle) {
        this.jobTitle = jobTitle;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }
}
