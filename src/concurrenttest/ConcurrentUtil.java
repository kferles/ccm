package concurrenttest;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class ConcurrentUtil {

    public static int waitOnBarrier(CyclicBarrier barrier){
        try {
            return barrier.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }

        return -1;
    }

    public static void joinWithThread(Thread t){
        try{
            t.join();
        }
        catch(InterruptedException _){
            Thread.currentThread().interrupt();
        }
    }
}
