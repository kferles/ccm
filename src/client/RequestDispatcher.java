package client;

import exception.InvalidRecordException;
import exception.InvalidRecordSize;
import exception.RemoteFailure;
import file.record.Identifiable;
import file.record.SerializableRecord;
import server.RequestType;
import server.ResponseType;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;

public class RequestDispatcher<K extends Comparable<K>, R extends Identifiable<K> &
                                                                  SerializableRecord &
                                                                  Serializable> {

    private final Socket commSocket;

    private ObjectOutputStream objOutStream;

    private ObjectInputStream objInStream;

    private final Class<R> recordClass;

    private void checkForFailureOrRestart(ResponseType res) throws IOException, ClassNotFoundException, RemoteFailure {
        if(res == ResponseType.UNEXPECTED_FAILURE ||
           res == ResponseType.INVALID_REQUEST){
            throw new RemoteFailure((String) objInStream.readObject(), res);
        }

        if(res == ResponseType.RESTART){
            throw new RemoteFailure(null, res);
        }
    }

    public RequestDispatcher(Socket commSocket, Class<R> recordClass) throws IOException {
        this.commSocket = commSocket;
        this.objOutStream = new ObjectOutputStream(
                                    new BufferedOutputStream(commSocket.getOutputStream()));
        objOutStream.flush();
        this.objInStream = new ObjectInputStream(
                                    new BufferedInputStream(commSocket.getInputStream()));
        this.recordClass = recordClass;
    }

    public void beginXaction() throws IOException, ClassNotFoundException, RemoteFailure {
        objOutStream.writeObject(RequestType.BEGIN_XACTION);
        objOutStream.flush();

        ResponseType res = (ResponseType) objInStream.readObject();
        checkForFailureOrRestart(res);
        //this should be a check, but this ok for now
        assert res == ResponseType.BEGIN_XACTION_SUCCESS;
    }

    public void endXaction() throws IOException, ClassNotFoundException, RemoteFailure {
        objOutStream.writeObject(RequestType.END_XACTION);
        objOutStream.flush();

        ResponseType res = (ResponseType) objInStream.readObject();
        checkForFailureOrRestart(res);
        assert res == ResponseType.END_XACTION_SUCCESS;
    }

    public void insert(R rec) throws IOException, ClassNotFoundException, RemoteFailure, InvalidRecordException {
        objOutStream.writeObject(RequestType.INSERT);
        objOutStream.writeObject(rec);
        objOutStream.flush();

        ResponseType res = (ResponseType) objInStream.readObject();
        checkForFailureOrRestart(res);

        if(res == ResponseType.INSERT_KEY_EXISTS){
            throw new InvalidRecordException((String)objInStream.readObject());
        }

        assert res == ResponseType.INSERT_SUCCESS;
    }

    public void delete(K key) throws IOException, ClassNotFoundException, RemoteFailure, InvalidRecordException {
        objOutStream.writeObject(RequestType.DELETE);
        objOutStream.writeObject(key);
        objOutStream.flush();

        ResponseType res = (ResponseType) objInStream.readObject();
        checkForFailureOrRestart(res);

        if(res == ResponseType.DELETE_KEY_DOES_NOT_EXIST){
            throw new InvalidRecordException((String)objInStream.readObject());
        }

        assert res == ResponseType.DELETE_SUCCESS;
    }

    public R get(K key) throws IOException, ClassNotFoundException, RemoteFailure, InvalidRecordSize {
        objOutStream.writeObject(RequestType.LOOK_UP);
        objOutStream.writeObject(key);
        objOutStream.flush();

        ResponseType res = (ResponseType) objInStream.readObject();
        checkForFailureOrRestart(res);

        if(res == ResponseType.GET_KEY_DOES_NOT_EXISTS)
            return null;

        if(res == ResponseType.GET_FAILURE)
            throw new InvalidRecordSize((String) objInStream.readObject());

        assert res == ResponseType.GET_SUCCESS;

        return recordClass.cast(objInStream.readObject());
    }

    public void update(R record) throws IOException, ClassNotFoundException, InvalidRecordException, RemoteFailure {
        objOutStream.writeObject(RequestType.UPDATE);
        objOutStream.writeObject(record);
        objOutStream.flush();

        ResponseType res = (ResponseType) objInStream.readObject();
        checkForFailureOrRestart(res);

        if(res == ResponseType.UPDATE_FAILURE)
            throw new InvalidRecordException((String) objInStream.readObject());

        assert res == ResponseType.UPDATE_SUCESS;
    }

    @SuppressWarnings(value = "unchecked")
    public ArrayList<R> recordsInRange(K key1, K key2) throws IOException, ClassNotFoundException, InvalidRecordSize, RemoteFailure {
        objOutStream.writeObject(RequestType.RANGE);
        objOutStream.writeObject(key1);
        objOutStream.writeObject(key2);
        objOutStream.flush();

        ResponseType res = (ResponseType) objInStream.readObject();
        checkForFailureOrRestart(res);

        if(res == ResponseType.RANGE_FAILURE)
            throw new InvalidRecordSize((String) objInStream.readObject());

        assert res == ResponseType.RANGE_SUCCESS;

        return (ArrayList<R>) objInStream.readObject();
    }

    public void commit() throws IOException, ClassNotFoundException, RemoteFailure {
        objOutStream.writeObject(RequestType.COMMIT);
        objOutStream.flush();

        ResponseType res = (ResponseType) objInStream.readObject();
        checkForFailureOrRestart(res);

        assert res == ResponseType.COMMIT_SUCCESS;
    }

    public void rollback() throws IOException, RemoteFailure, ClassNotFoundException {
        objOutStream.writeObject(RequestType.ROLLBACK);
        objOutStream.flush();

        ResponseType res = (ResponseType) objInStream.readObject();
        checkForFailureOrRestart(res);

        assert res == ResponseType.ROLLBACK_SUCCESS;
    }

    public void closeStreams(){
        try {
            if(!this.commSocket.isClosed())
                this.commSocket.close();

            if(this.objInStream != null){
                this.objInStream.close();
                this.objInStream = null;
            }

            if(this.objOutStream != null){
                this.objOutStream.close();
                this.objOutStream = null;
            }
        } catch (IOException e) {
            System.err.println("Error while closing communication with server");
        }
    }
}
