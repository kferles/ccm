package client;

import exception.RemoteFailure;
import server.ResponseType;

import java.io.IOException;

public class XactionExecutor {

    public static void executeXaction(XactionTask task){
        begin:
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
                    break begin;
            }
        }
        task.closeConnections();
    }

}
