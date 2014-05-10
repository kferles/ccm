package exception;

import server.ResponseType;

public class RemoteFailure extends Exception {

    private ResponseType response;

    public RemoteFailure(String msg, ResponseType response){
        super(msg);
        this.response = response;
    }

    public ResponseType getResponse(){
        return this.response;
    }
}
