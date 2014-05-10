package client.sample;

import client.RequestDispatcher;
import client.XactionTask;
import file.record.sample.EmployeeRecord;

import java.io.IOException;
import java.net.Socket;

public abstract class BaseXactionTask implements XactionTask{

    protected RequestDispatcher<Integer, EmployeeRecord> dispatcher;

    public BaseXactionTask(String host, int port) throws IOException {
        Socket socket = new Socket(host, port);
        dispatcher = new RequestDispatcher<>(socket, EmployeeRecord.class);
    }

}
