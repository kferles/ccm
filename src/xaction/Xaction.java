package xaction;

import exception.InvalidBlockException;
import file.blockfile.Block;

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

    public static boolean isXactionExecuting(Xaction xaction){
        return activeXactions.containsKey(xaction.executingThread.getId());
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

    public void commit() throws IOException, InvalidBlockException {
        for(FileChannel channel : usingBlocks.keySet()){
            Map<Integer, Block> blks = usingBlocks.get(channel);
            for(Integer num : blks.keySet()){
                Block block = blks.get(num);
                block.commit();
            }
        }
    }

    public void rollback() throws IOException {
        //TODO: do not release locks for metadata before forceDispose calls
        List<Block> newBlocks = new ArrayList<>();
        for(FileChannel channel : usingBlocks.keySet()){
            Map<Integer, Block> blocks = usingBlocks.get(channel);
            for(Integer num : blocks.keySet()){
                Block block = blocks.get(num);
                if(block.isNewBlock())
                    newBlocks.add(block);
                block.invalidate();
            }
        }

        for(Block newBlock : newBlocks){
            try {
                newBlock.forceDispose();
            } catch (InvalidBlockException _) {
                assert false;
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
