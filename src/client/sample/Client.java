package client.sample;

import client.XactionExecutor;
import exception.InvalidRecordException;
import exception.InvalidRecordSize;
import exception.RemoteFailure;
import file.record.sample.EmployeeRecord;

import java.io.IOException;

public class Client implements Runnable{

    private BaseXactionTask task;

    public Client(BaseXactionTask task){
        this.task = task;
    }

    @Override
    public void run() {
        XactionExecutor.executeXaction(task);
    }

    public static void main(String[] args){
        try {
            Client cl = new Client(new BaseXactionTask("localhost", 2345) {
                @Override
                public void runXaction() throws IOException, ClassNotFoundException, RemoteFailure {
                    beginXaction();
                    try {
                        insert(new EmployeeRecord(1, "Katerina", "Zamani"));
                        insert(new EmployeeRecord(7, "Kostas", "Ferles"));
                        System.out.println(recordsInRange(0, 6));
                        commit();
                    }
                    catch (InvalidRecordException | InvalidRecordSize invalidRecordSize) {
                        rollback();
                        invalidRecordSize.printStackTrace();
                    }
                    endXaction();
                }
            });

            cl.run();
        } catch (IOException e) {
            System.err.println("Error starting client: " + e.getMessage());
        }
    }
}
