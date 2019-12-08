package org.arpit.spark.stream00.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.arpit.spark.common.util.Employee;

import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketProducer {

    public static void main(String[] args) throws Exception {
        ServerSocket echoSocket = new ServerSocket(10000);
        Socket socket = echoSocket.accept();
        PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);

        while (true) {
            Employee employee = Employee.buildRandomEmployee();
            String employeeJson = new ObjectMapper().writeValueAsString(employee);
            printWriter.println(employeeJson);
            System.out.println("Employee id : " + employee.getId());

            Thread.sleep(500);
        }
    }
}
