package xaction;

import exception.InvalidBlockExcepxtion;
import file.blockfile.Block;
import file.blockfile.BlockFile;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Xaction {

    private static final ConcurrentMap<Long, Xaction> activeXactions = new ConcurrentHashMap<>();

    public static Xaction getExecutingXaction(){
        return activeXactions.get(Thread.currentThread().getId());
    }

    private Thread executingThread;

    private long id;

    Map<FileChannel, Map<Integer, Block>> usingBlocks = new HashMap<>();

    public Xaction(){
        this.executingThread = Thread.currentThread();
        this.id = executingThread.getId();
    }

    public void begin(){
        Xaction prev = activeXactions.put(id, this);
        assert prev == null;
    }

    public void end(){
        activeXactions.remove(id);
        usingBlocks.clear();
    }

    public void commit() throws IOException, InvalidBlockExcepxtion {
        for(FileChannel channel : usingBlocks.keySet()){
            Map<Integer, Block> blks = usingBlocks.get(channel);
            for(Integer num : blks.keySet()){
                Block block = blks.get(num);
                BlockFile bf = block.getBlockFile();
                bf.commitBlock(block);
            }
        }
    }

    public void addBlock(FileChannel channel, Block block){
        assert Thread.currentThread().getId() == this.id;

        if(!usingBlocks.containsKey(channel))
            usingBlocks.put(channel, new HashMap<Integer, Block>());

        int blockNum = block.getBlockNum();

        assert this.usingBlocks.get(channel).get(blockNum) == null;
        this.usingBlocks.get(channel).put(blockNum, block);
    }

    public boolean contains(FileChannel channel, int blockNum){
        if(!usingBlocks.containsKey(channel))
            return false;

        return usingBlocks.get(channel).containsKey(blockNum);
    }

    public Block get(FileChannel channel, int blockNum){
        if(!usingBlocks.containsKey(channel))
            return null;

        return usingBlocks.get(channel).get(blockNum);
    }

    public long getId(){
        return this.id;
    }

}
