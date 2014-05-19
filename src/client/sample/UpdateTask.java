package client.sample;

import exception.InvalidRecordException;
import exception.RemoteFailure;
import file.record.sample.EmployeeRecord;

import java.io.IOException;

public class UpdateTask extends BaseXactionTask {

    private EmployeeRecord rec;

    public UpdateTask(String host, int port, EmployeeRecord rec) throws IOException {
        super(host, port);
        this.rec = rec;
    }

    @Override
    public void runXaction() throws IOException, ClassNotFoundException, RemoteFailure {
        beginXaction();
        try{
            update(rec);
            commit();
        }
        catch(InvalidRecordException _){
            rollback();
        }
        endXaction();

    }

}