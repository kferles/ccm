package client.sample;

import exception.InvalidRecordSize;
import exception.RemoteFailure;
import file.record.sample.EmployeeRecord;

import java.io.IOException;
import java.util.ArrayList;

public class RangeTask extends BaseXactionTask {

    private int key1;
    private int key2;

    public RangeTask(String host, int port, int key1, int key2) throws IOException {
        super(host, port);
        this.key1 = key1;
        this.key2 = key2;
    }

    @Override
    public void runXaction() throws IOException, ClassNotFoundException, RemoteFailure {
        beginXaction();
        try {
            ArrayList<EmployeeRecord> list = recordsInRange(key1, key2);
            System.out.println("Results found:");
            System.out.println(list);
            commit();
        } catch (InvalidRecordSize e) {
            e.printStackTrace();
            rollback();
        }
        endXaction();

    }
}