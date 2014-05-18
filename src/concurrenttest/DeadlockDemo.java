package concurrenttest;

import client.sample.BaseXactionTask;
import client.sample.Client;
import client.sample.ConcurrentClient;
import exception.InvalidRecordException;
import exception.InvalidRecordSize;
import exception.RemoteFailure;
import file.record.sample.EmployeeRecord;

import java.io.IOException;
import java.util.concurrent.CyclicBarrier;

import static concurrenttest.ConcurrentUtil.joinWithThread;

public class DeadlockDemo {


    public static void main(String[] args){
        try {
            final CyclicBarrier barrier = new CyclicBarrier(2);
            final ConcurrentClient c1 = new ConcurrentClient(new BaseXactionTask("localhost", 2345) {
                @Override
                public void runXaction() throws IOException, ClassNotFoundException, RemoteFailure {
                    try {

                        beginXaction();
                        insert(new EmployeeRecord(1, "Kostas", "Ferles"));
                        commit();
                    } catch (InvalidRecordException e) {
                        rollback();
                    }

                    endXaction();
                }
            }, barrier);

            final ConcurrentClient c2 = new ConcurrentClient(new BaseXactionTask("localhost", 2345) {
                @Override
                public void runXaction() throws IOException, ClassNotFoundException, RemoteFailure {
                    try {

                        beginXaction();
                        insert(new EmployeeRecord(2, "Katerina", "Zamani"));
                        commit();
                    } catch (InvalidRecordException e) {
                        rollback();
                    }

                    endXaction();
                }
            }, barrier);

            Thread t1 = new Thread(c1), t2 = new Thread(c2);
            t1.start();
            t2.start();

            joinWithThread(t1);
            joinWithThread(t2);

            Client c3 = new Client(new BaseXactionTask("localhost", 2345) {
                @Override
                public void runXaction() throws IOException, ClassNotFoundException, RemoteFailure {
                    try {
                        beginXaction();
                        System.out.println(recordsInRange(0, 3));
                        commit();
                    } catch (InvalidRecordSize e) {
                        rollback();
                    }

                    endXaction();
                }
            });

            c3.run();

        }
        catch(IOException e){
            e.printStackTrace();
        }
    }
}
