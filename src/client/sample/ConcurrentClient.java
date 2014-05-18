package client.sample;

import client.XactionExecutor;

import java.util.concurrent.CyclicBarrier;

import static concurrenttest.ConcurrentUtil.waitOnBarrier;

public class ConcurrentClient implements Runnable{

    private BaseXactionTask task;

    private CyclicBarrier barrier;

    public ConcurrentClient(BaseXactionTask task, CyclicBarrier barrier){
        this.task = task;
        this.barrier = barrier;
    }

    @Override
    public void run() {
        waitOnBarrier(barrier);
        XactionExecutor.executeXaction(task);
    }
}
