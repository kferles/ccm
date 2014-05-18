package concurrenttest;

import client.sample.BaseXactionTask;
import client.sample.ConcurrentClient;
import exception.InvalidRecordException;
import exception.RemoteFailure;
import file.record.sample.EmployeeRecord;

import java.io.IOException;
import java.util.concurrent.CyclicBarrier;

public class ConcurrentInsertClient extends ConcurrentClient {

    public ConcurrentInsertClient(CyclicBarrier barrier, final Integer id) throws IOException {
        super(new BaseXactionTask("localhost", 2345) {
            @Override
            public void runXaction() throws IOException, ClassNotFoundException, RemoteFailure {
                beginXaction();
                try{
                    insert(new EmployeeRecord(id, "firstName", "lastName"));
                    commit();
                }
                catch(InvalidRecordException _){
                    rollback();
                }
                endXaction();
            }
        }, barrier);
    }
}
