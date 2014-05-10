package client;

import exception.RemoteFailure;

import java.io.IOException;

public interface XactionTask {

    public void runXaction() throws IOException, ClassNotFoundException,
                                    RemoteFailure;
}
