package client.sample;

import exception.InvalidRecordException;
import exception.RemoteFailure;
import file.record.sample.EmployeeRecord;

import java.io.IOException;

public class InsertTask extends BaseXactionTask {

    private EmployeeRecord rec;

    public InsertTask(String host, int port, EmployeeRecord rec) throws IOException {
        super(host, port);
        this.rec = rec;
    }

    @Override
    public void runXaction() throws IOException, ClassNotFoundException, RemoteFailure {
        beginXaction();
        try{
            insert(rec);
            commit();
        }
        catch(InvalidRecordException _){
            rollback();
        }
        endXaction();

    }
}