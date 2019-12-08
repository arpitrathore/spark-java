package org.arpit.spark.stream00.common;

import org.arpit.spark.common.util.Employee;

import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;

public class C01SocketProducer {

    private static int SOCKET_PORT = 10000;

    public static void main(String[] args) throws Exception {
        System.out.println("C01SocketProducer started. Waiting for consumer....");
        ServerSocket echoSocket = new ServerSocket(SOCKET_PORT);
        Socket socket = echoSocket.accept();
        PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);

        while (true) {
            String employeeJson = Employee.buildRandomEmployeeJson();
            printWriter.println(employeeJson);
            System.out.println("Sent employee to socket port " + employeeJson + " at : " + new Date());

            Thread.sleep(500);
        }
    }
}
