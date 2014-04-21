package xaction;

import exception.InvalidBlockExcepxtion;
import file.blockfile.Block;
import file.blockfile.BlockFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Xaction {

    private static final ConcurrentMap<Long, Xaction> activeXactions = new ConcurrentHashMap<>();

    public static Xaction getExecutingXaction(){
        return activeXactions.get(Thread.currentThread().getId());
    }

    private Thread executingThread;

    private long id;

    private Set<Block> usedBlocks = new HashSet<>();

    private List<Block> newBlocks = new ArrayList<>();

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
        usedBlocks.clear();
        newBlocks.clear();
    }

    public void commit() throws IOException, InvalidBlockExcepxtion {
        List<Block> metadataBlocks = new ArrayList<>();
        for(Block b : newBlocks){
            BlockFile bf = b.getBlockFile();
            bf.commitBlock(b);
        }
        for(Block b : usedBlocks){
            if(b.getBlockNum() != 0){
                BlockFile bf = b.getBlockFile();
                bf.commitBlock(b);
            }
            else{
                metadataBlocks.add(b);
            }
        }

        for(Block metadataBlock : metadataBlocks){
            BlockFile bf = metadataBlock.getBlockFile();
            bf.commitBlock(metadataBlock);
        }
    }

    public void addBlock(Block block){
        assert Thread.currentThread().getId() == this.id;
        if(block.getBlockNum() != -1)
            this.usedBlocks.add(block);
        else
            this.newBlocks.add(block);
    }

    public boolean contains(Block block){
        return usedBlocks.contains(block);
    }

    public long getId(){
        return this.id;
    }

}
