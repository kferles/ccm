package concurrenttest;

import client.sample.ConcurrentClient;
import client.sample.InsertTask;
import file.record.sample.EmployeeRecord;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;

public class ConcurrentInsert {

    public static void main(String[] args) throws IOException {
        Random r = new Random();
        Set<Integer> ids = new HashSet<>();
        final int totalRecs = Integer.parseInt(args[0]);
        final CyclicBarrier barrier = new CyclicBarrier(totalRecs);
        for(int i = 0; i < totalRecs; ++i){
            Integer id = r.nextInt();
            while(ids.contains(id))
                id = r.nextInt();
            ids.add(id);
            new Thread(new ConcurrentClient(new InsertTask("localhost", 2345, new EmployeeRecord(id, "firstname", "lastname")), barrier)).start();
        }
    }
}
