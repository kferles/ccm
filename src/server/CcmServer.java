package server;

import exception.InvalidKeyFactoryException;
import exception.InvalidRecordSize;
import file.index.BPlusIndex;
import file.record.sample.EmployeeFactory;
import file.record.sample.EmployeeKeyValFactory;
import file.record.sample.EmployeeRecord;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class CcmServer implements Runnable {

    private final int port;

    private BPlusIndex<Integer, EmployeeRecord> index;

    private final ServerSocket socket;

    private volatile boolean stop = false;

    public CcmServer(int port, BPlusIndex<Integer, EmployeeRecord> index) throws IOException {
        this.port = port;
        this.socket = new ServerSocket(port);
        this.index = index;
    }

    @Override
    public void run() {

        while(!stop){
            try {
                Socket clientSocket = this.socket.accept();
                ClientHandler<Integer, EmployeeRecord> handler = new ClientHandler<>(clientSocket, index,
                                                                                     EmployeeRecord.class, Integer.class);

                new Thread(handler).start();
            } catch (IOException e) {
                if(stop){
                    return;
                }
                System.err.println("Error accepting client: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args){
        try {
            EmployeeFactory recFac = new EmployeeFactory();
            EmployeeKeyValFactory keyFac = new EmployeeKeyValFactory();
            BPlusIndex<Integer, EmployeeRecord> index = new BPlusIndex<>("first_test", false, recFac, keyFac);
            CcmServer server = new CcmServer(2345, index);
            server.run();
        } catch (Exception e) {
            System.err.println("failed to start server: " + e.getMessage());
        }
    }
}
