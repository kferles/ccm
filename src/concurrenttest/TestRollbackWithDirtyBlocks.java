package concurrenttest;

import client.sample.BaseXactionTask;
import client.sample.Client;
import exception.InvalidRecordException;
import exception.InvalidRecordSize;
import exception.RemoteFailure;
import file.record.sample.EmployeeRecord;

import java.io.IOException;
import java.util.Random;

public class TestRollbackWithDirtyBlocks {


    public static void main(String[] args) throws IOException {
        new Client(new BaseXactionTask("localhost", 2345) {
            @Override
            public void runXaction() throws IOException, ClassNotFoundException, RemoteFailure {
                beginXaction();
                Random r = new Random();

                Integer id = r.nextInt();
                action:{
                    try {
                        while(get(id) != null)
                            id = r.nextInt();
                    } catch (InvalidRecordSize invalidRecordSize) {
                        invalidRecordSize.printStackTrace();
                        rollback();
                        break action;
                    }

                    System.out.println("Performing Double Insert: " + id);
                    try {
                        insert(new EmployeeRecord(id, "Test", "Test"));
                        insert(new EmployeeRecord(id, "Test", "Test"));
                    } catch (InvalidRecordException e) {
                        e.printStackTrace();
                        rollback();
                        break action;
                    }
                    commit();
                }

                endXaction();

                final Integer idToLookup = id;
                new Client(new BaseXactionTask("localhost", 2345) {
                    @Override
                    public void runXaction() throws IOException, ClassNotFoundException, RemoteFailure {
                        beginXaction();
                        try {
                            System.out.println(get(idToLookup));
                            commit();
                        } catch (InvalidRecordSize invalidRecordSize) {
                            rollback();
                            invalidRecordSize.printStackTrace();
                        }
                        endXaction();
                    }
                }).run();

            }
        }).run();
    }
}
