package server;

import config.ConfigParameters;
import deadlockmanager.DeadlockManager;
import exception.InvalidKeyFactoryException;
import exception.InvalidRecordSize;
import file.index.BPlusIndex;
import file.record.sample.EmployeeFactory;
import file.record.sample.EmployeeKeyValFactory;
import file.record.sample.EmployeeRecord;
import util.FileSystemMethods;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class CcmServer implements Runnable {

    private BPlusIndex<Integer, EmployeeRecord> index;

    private final ServerSocket socket;

    public static volatile boolean stop = false;

    public CcmServer(int port, BPlusIndex<Integer, EmployeeRecord> index) throws IOException {
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
            String recordFile = null;
            int bufferSize = 1024;
            int buffersNum = 500;
            int port = 2345;

            ConfigParameters configParameters = ConfigParameters.getInstance();

            for(int i = 0; i < args.length; ++i){
                String arg = args[i];
                switch(arg){
                    case "-f":
                        recordFile = args[++i];
                        break;
                    case "-bs":
                        bufferSize = Integer.parseInt(args[++i]);
                        break;
                    case "-bn":
                        buffersNum = Integer.parseInt(args[++i]);
                        break;
                    case "-p":
                        port = Integer.parseInt(args[++i]);
                        break;
                    case "-d":
                        configParameters.setDurable(true);
                        break;
                    default:
                        System.err.println(args[i] + ": invalid argument");
                        System.exit(1);
                }
            }

            if(recordFile == null){
                System.err.println("missing record file");
                System.exit(1);
            }

            configParameters.setBufferSize(bufferSize);
            configParameters.setMaxBuffNumber(buffersNum);

            boolean initializeIndex = !FileSystemMethods.exists(FileSystemMethods.getPath(recordFile));

            EmployeeFactory recFac = new EmployeeFactory();
            EmployeeKeyValFactory keyFac = new EmployeeKeyValFactory();
            BPlusIndex<Integer, EmployeeRecord> index = new BPlusIndex<>(recordFile, initializeIndex, recFac, keyFac);

            final CcmServer server = new CcmServer(port, index);
            final Thread mainThread = Thread.currentThread();
            final Thread deadlockManagerThread = new Thread(new DeadlockManager());

            Runtime.getRuntime().addShutdownHook(new Thread(){
                @Override
                public void run(){
                    try {
                        System.out.println("Running shutdown hook");
                        System.out.flush();
                        stop = true;
                        server.socket.close();
                        try {
                            mainThread.join();
                        } catch (InterruptedException e) {
                            mainThread.interrupt();
                        }

                        try {
                            deadlockManagerThread.join();
                        } catch (InterruptedException e) {
                            deadlockManagerThread.interrupt();
                        }
                    }
                    catch (IOException e){
                        e.printStackTrace();
                    }
                }
            });

            deadlockManagerThread.start();
            server.run();
        }
        catch(Exception e){
            System.err.println("failed to start server: " + e.getMessage());
        }
    }
}
