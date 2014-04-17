package buffermanager;

import config.ConfigParameters;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by kostas on 4/15/14.
 */
public class BufferManager {

    private static ConfigParameters config = ConfigParameters.getInstance();

    private static final int bufferSize = config.getBufferSize();

    private static BufferManager ourInstance = new BufferManager();

    private List<ByteBuffer> availableBuffers;

    private BufferManager(){
        int maxBuffNum = config.getMaxBuffNumber();
        this.availableBuffers = new ArrayList<>();
        for(int i = 0; i < maxBuffNum; i++){
            availableBuffers.add(ByteBuffer.allocateDirect(bufferSize));
        }
    }

    public static BufferManager getInstance(){
        return ourInstance;
    }

    public synchronized ByteBuffer getBuffer(){
        while(availableBuffers.isEmpty()){
            try {
                wait();
            }
            catch (InterruptedException _) { }
        }

        return availableBuffers.remove(0);
    }

    public synchronized void releaseBuffer(ByteBuffer buffer){
        availableBuffers.add(buffer);
        notify();
    }
}
