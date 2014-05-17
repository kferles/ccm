package server;

import exception.*;
import file.index.BPlusIndex;
import file.record.Identifiable;
import file.record.SerializableRecord;
import xaction.Xaction;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;

public class ClientHandler<K extends Comparable<K>, R extends SerializableRecord &
                                                              Identifiable<K> &
                                                              Serializable> implements Runnable {

    private BPlusIndex<K, R> index;

    private Xaction clientsXaction;

    private final Class<R> recordClass;

    private final Class<K> keyClass;

    private final Socket clientSocket;

    private final ObjectInputStream objInStream;

    private final ObjectOutputStream objOutStream;

    private RequestType lastReq = null;

    private void processRequest(RequestType req) throws InvalidRequestException, IOException,
                                                        ClassNotFoundException, InvalidBlockException {

        switch(req){
            case BEGIN_XACTION:
                if(Xaction.isXactionExecuting(clientsXaction))
                    throw new InvalidRequestException("Transaction has already started");
                assert lastReq == null;
                clientsXaction.begin();
                objOutStream.writeObject(ResponseType.BEGIN_XACTION_SUCCESS);
                break;
            case END_XACTION:
                if(lastReq != RequestType.COMMIT && lastReq != RequestType.ROLLBACK)
                    throw new InvalidRequestException("Transaction must either commit or rollback before ending");
                clientsXaction.end();
                objOutStream.writeObject(ResponseType.END_XACTION_SUCCESS);
                break;
            case INSERT:

                assert Xaction.isXactionExecuting(clientsXaction);

                R insRec = recordClass.cast(objInStream.readObject());

                try {
                    index.insert(insRec.getId(), insRec);
                }
                catch (InvalidRecordException e) {
                    objOutStream.writeObject(ResponseType.INSERT_KEY_EXISTS);
                    objOutStream.writeObject(e.getMessage());
                    break;
                }
                objOutStream.writeObject(ResponseType.INSERT_SUCCESS);
                break;
            case DELETE:

                assert Xaction.isXactionExecuting(clientsXaction);

                K delKey = keyClass.cast(objInStream.readObject());

                try{
                    index.delete(delKey);
                }
                catch(InvalidRecordException e){
                    objOutStream.writeObject(ResponseType.DELETE_KEY_DOES_NOT_EXIST);
                    objOutStream.writeObject(e.getMessage());
                    break;
                }

                objOutStream.writeObject(ResponseType.DELETE_SUCCESS);
                break;
            case LOOK_UP:

                assert Xaction.isXactionExecuting(clientsXaction);

                K lookupKey = keyClass.cast(objInStream.readObject());
                R getRec;

                try {
                    getRec = index.get(lookupKey);
                } catch (InvalidRecordSize invalidRecordSize) {
                    objOutStream.writeObject(ResponseType.GET_FAILURE);
                    objOutStream.writeObject(invalidRecordSize.getMessage());
                    break;
                }

                if(getRec == null){
                    objOutStream.writeObject(ResponseType.GET_KEY_DOES_NOT_EXISTS);
                }
                else{
                    objOutStream.writeObject(ResponseType.GET_SUCCESS);
                    objOutStream.writeObject(getRec);
                }
                break;
            case UPDATE:

                assert Xaction.isXactionExecuting(clientsXaction);

                R updateRec = recordClass.cast(objInStream.readObject());

                try {
                    index.update(updateRec);
                } catch (InvalidRecordException e) {
                    objOutStream.writeObject(ResponseType.UPDATE_FAILURE);
                    objOutStream.writeObject(e.getMessage());
                    break;
                }
                objOutStream.writeObject(ResponseType.UPDATE_SUCESS);
                break;
            case RANGE:

                assert Xaction.isXactionExecuting(clientsXaction);

                K rangeKeyLow = keyClass.cast(objInStream.readObject());
                K rangeKeyHigh = keyClass.cast(objInStream.readObject());
                ArrayList<R> rv;

                try {
                    rv = index.recordsInRange(rangeKeyLow, rangeKeyHigh);
                } catch (InvalidRecordSize invalidRecordSize) {
                    objOutStream.writeObject(ResponseType.RANGE_FAILURE);
                    objOutStream.writeObject(invalidRecordSize.getMessage());
                    break;
                }
                objOutStream.writeObject(ResponseType.RANGE_SUCCESS);
                objOutStream.writeObject(rv);
                break;
            case ROLLBACK:

                assert Xaction.isXactionExecuting(clientsXaction);

                try{
                    clientsXaction.rollback();
                    objOutStream.writeObject(ResponseType.ROLLBACK_SUCCESS);
                }
                catch(Exception e){
                    objOutStream.writeObject(ResponseType.UNEXPECTED_FAILURE);
                    objOutStream.writeObject("Error during rollback");
                    System.err.println("Something went really bad: " + e.getMessage());
                }
                break;
            case COMMIT:

                assert Xaction.isXactionExecuting(clientsXaction);

                try{
                    clientsXaction.commit();
                    objOutStream.writeObject(ResponseType.COMMIT_SUCCESS);
                }catch(Exception e){
                    objOutStream.writeObject(ResponseType.UNEXPECTED_FAILURE);
                    objOutStream.writeObject("Error during commit");
                    System.err.println("Something went really bad: " + e.getMessage());
                }
                break;
        }
        objOutStream.flush();
        lastReq = req;
    }

    public ClientHandler(Socket clientSocket, BPlusIndex<K, R> index,
                         Class<R> recordClass, Class<K> keyClass) throws IOException {
        this.clientSocket = clientSocket;
        this.objOutStream = new ObjectOutputStream(
                new BufferedOutputStream(this.clientSocket.getOutputStream()));
        this.objOutStream.flush();
        this.objInStream = new ObjectInputStream(
                                new BufferedInputStream(this.clientSocket.getInputStream()));
        this.recordClass = recordClass;
        this.keyClass = keyClass;
        this.index = index;
    }

    @Override
    public void run() {

        clientsXaction = new Xaction();

        RequestType req;
        ResponseType finalResponse = null;
        String finalMsg = null;
        try{
            req = (RequestType) objInStream.readObject();
            while(req != RequestType.END_XACTION){
                processRequest(req);
                req = (RequestType) objInStream.readObject();
            }
            processRequest(req);
        }
        catch(InvalidRequestException invalidReq){
            finalResponse = ResponseType.INVALID_REQUEST;
            finalMsg = invalidReq.getMessage();
        }
        catch(ClassCastException | ClassNotFoundException noClass){
            finalResponse = ResponseType.INVALID_REQUEST;
            finalMsg = "Invalid request";
        }
        catch(RestartException _){
            finalResponse = ResponseType.RESTART;
        }
        catch(Exception _){
            finalResponse = ResponseType.UNEXPECTED_FAILURE;
            finalMsg = "Server terminated unexpectedly";
        }

        try {
            if(finalResponse != null){
                clientsXaction.rollback();
                clientsXaction.end();
                objOutStream.writeObject(finalResponse);
                if(finalMsg != null)
                    objOutStream.writeObject(finalMsg);
                objOutStream.flush();
            }
            objOutStream.close();
            objInStream.close();
            clientSocket.close();
        } catch (IOException e) {
            System.err.println("Error terminating communication " + e.getMessage());
        }
    }

}
