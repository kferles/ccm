package client.sample;

import exception.InvalidRecordException;
import exception.RemoteFailure;

import java.io.IOException;

public class DeleteFileTask  extends BaseXactionTask{

    private String filename;

    public DeleteFileTask(String host, int port, String filename) throws IOException {
        super(host, port);
        this.filename = filename;
    }

    @Override
    public void runXaction() throws IOException, ClassNotFoundException, RemoteFailure {
        beginXaction();
        try{
            deleteFromFile(filename);
            commit();
        }
        catch(InvalidRecordException _){
            rollback();
        }
        endXaction();

    }

}