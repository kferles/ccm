package client;

import exception.RemoteFailure;
import server.ResponseType;

import java.io.IOException;
import java.util.Random;

public class XactionExecutor {

    public static void executeXaction(XactionTask task){
        try {
            task.runXaction();
        }
        catch (ClassNotFoundException | IOException e) {
            System.err.println("Unexpected error: " + e.getMessage());
        }
        catch (RemoteFailure remoteFailure) {
            ResponseType response = remoteFailure.getResponse();
            switch(response){
                case UNEXPECTED_FAILURE:
                case INVALID_REQUEST:
                    System.err.println("Sever communication error: " + remoteFailure.getMessage());
                    break;
                case RESTART:
                    try {
                        Random r = new Random();
                        Thread.sleep(r.nextInt(5000) + 1);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    executeXaction(task);
                    break;
            }
        }
        task.closeConnections();
    }

}
