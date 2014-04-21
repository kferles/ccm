package buffermanager;

import config.ConfigParameters;
import exception.InvalidBlockExcepxtion;
import file.blockfile.Block;
import file.blockfile.BlockFile;
import util.Pair;
import xaction.Xaction;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by kostas on 4/15/14.
 */
public class BufferManager {

    private static ConfigParameters config = ConfigParameters.getInstance();

    private static final int bufferSize = config.getBufferSize();

    private static BufferManager ourInstance = new BufferManager();

    public static BufferManager getInstance(){
        return ourInstance;
    }

    private List<ByteBuffer> availableBuffers;

    private Map<FileChannel, Map<Integer, Block>> cleanBlocks = new HashMap<>();

    private List<Pair<FileChannel, Integer>>  victimizePool = new ArrayList<>();

    private BufferManager(){
        int maxBuffNum = config.getMaxBuffNumber();
        this.availableBuffers = new ArrayList<>();
        for(int i = 0; i < maxBuffNum; i++){
            availableBuffers.add(ByteBuffer.allocateDirect(bufferSize));
        }
    }

    private ByteBuffer victimize(){
        Pair<FileChannel, Integer> victim = victimizePool.remove(victimizePool.size() - 1);
        Map<Integer, Block> blocks = cleanBlocks.get(victim.first);
        return blocks.remove(victim.second).getBuffer();
    }

    private Block checkCachedBlocks(FileChannel channel, Integer number){
        if(cleanBlocks.containsKey(channel)){
            Map<Integer, Block> blockMap = cleanBlocks.get(channel);
            if(blockMap.containsKey(number))
                return blockMap.remove(number);
        }
        return null;
    }

    public synchronized Block getBlock(BlockFile bf, Integer number, boolean newBlock) throws IOException {

        Xaction currXaction = Xaction.getExecutingXaction();

        assert currXaction != null;

        FileChannel channel = bf.getChannel();

        if(!newBlock){
            Block rv = checkCachedBlocks(channel, number);
            if(rv != null){
               currXaction.addBlock(rv);
               return rv;
            }
        }

        ByteBuffer buff = null;

        while(victimizePool.isEmpty() && availableBuffers.isEmpty()){
            try {
                wait();
            }
            catch (InterruptedException _) { }
        }

        if(!newBlock){
            Block rv = checkCachedBlocks(channel, number);
            if(rv != null){
                currXaction.addBlock(rv);
                return rv;
            }
        }

        if(!availableBuffers.isEmpty())
            buff = availableBuffers.remove(0);
        else if(!victimizePool.isEmpty())
            buff = victimize();
        else
            assert false;

        if(!newBlock)
            channel.read(buff, number*bufferSize);
        Block rv = new Block(number, buff, bf, newBlock);

        currXaction.addBlock(rv);

        return rv;
    }

    public synchronized void releaseBlock(Block block){
        FileChannel channel = block.getChannel();
        Integer num = block.getBlockNum();

        assert !block.isDirty();

        if(!cleanBlocks.containsKey(channel)){
            cleanBlocks.put(channel, new HashMap<Integer, Block>());
        }

        Map<Integer, Block> blocks = cleanBlocks.get(channel);
        blocks.put(num, block);

        victimizePool.add(0, new Pair<>(channel, num));

        notify();
    }

    public synchronized void invalidateBlock(Block block) throws IOException, InvalidBlockExcepxtion {

        FileChannel channel = block.getChannel();
        Integer num = block.getBlockNum();

        if(cleanBlocks.containsKey(channel)){
            Map<Integer, Block> blocks = cleanBlocks.get(channel);
            if(blocks.containsKey(num)){
                blocks.remove(num);
                victimizePool.remove(new Pair<>(channel, num));
            }
        }

        if(block.isNewBlock() && !block.isDisposed()){
            BlockFile bf = block.getBlockFile();
            bf.disposeBlock(block);
            block.writeToFile();
            Block metadataBlock = bf.loadBlock(0);
            metadataBlock.writeToFile();
        }

        availableBuffers.add(block.getBuffer());

        notify();
    }
}
