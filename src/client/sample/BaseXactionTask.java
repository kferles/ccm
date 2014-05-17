package client.sample;

import client.RequestDispatcher;
import client.XactionTask;
import exception.InvalidRecordException;
import exception.InvalidRecordSize;
import exception.RemoteFailure;
import file.record.sample.EmployeeRecord;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;

public abstract class BaseXactionTask implements XactionTask{

    protected RequestDispatcher<Integer, EmployeeRecord> dispatcher;

    protected void beginXaction() throws RemoteFailure, IOException, ClassNotFoundException {
        dispatcher.beginXaction();
    }

    protected void endXaction() throws RemoteFailure, IOException, ClassNotFoundException {
        dispatcher.endXaction();
    }

    protected void insert(EmployeeRecord rec) throws ClassNotFoundException, InvalidRecordException, RemoteFailure, IOException {
        dispatcher.insert(rec);
    }

    protected void delete(Integer key) throws ClassNotFoundException, InvalidRecordException, RemoteFailure, IOException {
        dispatcher.delete(key);
    }

    protected EmployeeRecord get(Integer key) throws ClassNotFoundException, InvalidRecordSize, RemoteFailure, IOException {
        return dispatcher.get(key);
    }

    protected void update(EmployeeRecord rec) throws ClassNotFoundException, InvalidRecordException, RemoteFailure, IOException {
        dispatcher.update(rec);
    }

    protected ArrayList<EmployeeRecord> recordsInRange(Integer key1, Integer key2) throws ClassNotFoundException, InvalidRecordSize, RemoteFailure, IOException {
        return dispatcher.recordsInRange(key1, key2);
    }

    protected void commit() throws RemoteFailure, IOException, ClassNotFoundException {
        dispatcher.commit();
    }

    protected void rollback() throws RemoteFailure, IOException, ClassNotFoundException {
        dispatcher.rollback();
    }

    protected void wait(int secs){
        try {
            Thread.sleep(1000 * secs);
        }
        catch(InterruptedException e){
            Thread.currentThread().interrupt();
        }
    }

    public BaseXactionTask(String host, int port) throws IOException {
        Socket socket = new Socket(host, port);
        dispatcher = new RequestDispatcher<>(socket, EmployeeRecord.class);
    }

    @Override
    public void closeConnections() {
        dispatcher.closeStreams();
    }
}
