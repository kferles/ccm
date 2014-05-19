package client.sample;

import exception.InvalidRecordException;
import exception.RemoteFailure;

import java.io.IOException;

public class DeleteTask extends BaseXactionTask {

    private int key;

    public DeleteTask(String host, int port, int key) throws IOException {
        super(host, port);
        this.key = key;
    }

    @Override
    public void runXaction() throws IOException, ClassNotFoundException, RemoteFailure {
        beginXaction();
        try{
            delete(this.key);
            commit();
        }
        catch(InvalidRecordException _){
            rollback();
        }
        endXaction();

    }

}