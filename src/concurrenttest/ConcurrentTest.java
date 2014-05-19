package concurrenttest;

import client.sample.*;
import file.record.sample.EmployeeRecord;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CyclicBarrier;

public class ConcurrentTest {

    public static void main(String[] args) throws IOException {
        Random r = new Random();
        Set<Integer> ids = new HashSet<>();
        final int totalRecs = Integer.parseInt(args[0]);
        final CyclicBarrier barrier = new CyclicBarrier(totalRecs);
        List<Thread> threads = new ArrayList<>();
        System.out.println("Insert phase:");
        for(int i = 0; i < totalRecs; ++i){
            Integer id = r.nextInt();
            while(ids.contains(id))
                id = r.nextInt();
            ids.add(id);
            Thread insertThread = new Thread(new ConcurrentClient(new InsertTask("localhost", 2345, new EmployeeRecord(id, "firstname", "lastname")), barrier));
            insertThread.start();
            threads.add(insertThread);
        }

        for(Thread t : threads){
            ConcurrentUtil.joinWithThread(t);
        }

        getAll(ids, barrier, threads);

        barrier.reset();
        threads.clear();
        System.out.println("Update phase:");
        for(Integer id : ids){
            Thread updateThread = new Thread(new ConcurrentClient(new UpdateTask("localhost", 2345, new EmployeeRecord(id, "firstname1", "lastname1")), barrier));
            updateThread.start();
            threads.add(updateThread);
        }

        for(Thread t : threads){
            ConcurrentUtil.joinWithThread(t);
        }

        getAll(ids, barrier, threads);

        barrier.reset();
        threads.clear();

        System.out.println("Delete phase:");
        for(Integer id : ids){
            Thread getThread = new Thread(new ConcurrentClient(new DeleteTask("localhost", 2345, id), barrier));
            getThread.start();
            threads.add(getThread);
        }

        for(Thread t : threads){
            ConcurrentUtil.joinWithThread(t);
        }

        getAll(ids, barrier, threads);
    }

    private static void getAll(Set<Integer> ids, CyclicBarrier barrier, List<Thread> threads) throws IOException {
        barrier.reset();
        threads.clear();

        for(Integer id : ids){
            Thread getThread = new Thread(new ConcurrentClient(new GetTask("localhost", 2345, id), barrier));
            getThread.start();
            threads.add(getThread);
        }

        for(Thread t : threads){
            ConcurrentUtil.joinWithThread(t);
        }
    }
}
