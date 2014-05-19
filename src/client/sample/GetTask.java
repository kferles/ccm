package client.sample;

import exception.InvalidRecordSize;
import exception.RemoteFailure;
import file.record.sample.EmployeeRecord;

import java.io.IOException;

public class GetTask extends BaseXactionTask {

    private int key;
    public GetTask(String host, int port, int key) throws IOException {
        super(host, port);
        this.key = key;
    }

    @Override
    public void runXaction() throws IOException, ClassNotFoundException, RemoteFailure {
        beginXaction();
        try {
            EmployeeRecord rec = get(key);
            System.out.println(rec);
            commit();
        } catch (InvalidRecordSize e) {
            e.printStackTrace();
            rollback();
        }

        endXaction();

    }

}