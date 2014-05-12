package buffermanager;

import config.ConfigParameters;
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

    private Map<FileChannel, Map<Integer, Pair<Block, Integer>>> occupiedBlocks = new HashMap<>();

    private List<Pair<FileChannel, Integer>>  victimizePool = new ArrayList<>();

    private BufferManager(){
        int maxBuffNum = config.getMaxBuffNumber();
        this.availableBuffers = new ArrayList<>();
        for(int i = 0; i < maxBuffNum; i++){
            availableBuffers.add(ByteBuffer.allocateDirect(bufferSize));
        }
    }

    private ByteBuffer victimize(){
        Pair<FileChannel, Integer> victim = victimizePool.remove(0);
        Map<Integer, Block> blocks = cleanBlocks.get(victim.first);
        Block blk = blocks.remove(victim.second);

        assert !victimizePool.contains(victim);
        assert !blk.isDirty();

        return blk.getBuffer();
    }

    private Block checkCachedBlocks(FileChannel channel, Integer number){
        if(occupiedBlocks.containsKey(channel)){
            Map<Integer, Pair<Block, Integer>> blocksToXactions = occupiedBlocks.get(channel);
            if(blocksToXactions.containsKey(number)){
                Pair<Block, Integer> refCountBlock = blocksToXactions.get(number);
                refCountBlock.second++;
                return refCountBlock.first;
            }
        }
        if(cleanBlocks.containsKey(channel)){
            Map<Integer, Block> blockMap = cleanBlocks.get(channel);
            if(blockMap.containsKey(number)){
                Block rv = blockMap.remove(number);
                if(!occupiedBlocks.containsKey(channel))
                    occupiedBlocks.put(channel, new HashMap<Integer, Pair<Block, Integer>>());

                occupiedBlocks.get(channel).put(number, new Pair<>(rv, 1));
                assert victimizePool.contains(new Pair<>(channel, number));
                victimizePool.remove(new Pair<>(channel, number));
                return rv;
            }
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
                currXaction.addBlock(channel, rv);
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

        if(!availableBuffers.isEmpty())
            buff = availableBuffers.remove(0);
        else if(!victimizePool.isEmpty())
            buff = victimize();
        else
            assert false;

        if(!newBlock){
            buff.position(0);
            buff.limit(bufferSize);
            channel.read(buff, number*bufferSize);
        }

        Block rv = new Block(number, buff, bf, newBlock);

        if(!occupiedBlocks.containsKey(channel))
            occupiedBlocks.put(channel, new HashMap<Integer, Pair<Block, Integer>>());

        occupiedBlocks.get(channel).put(number, new Pair<>(rv, 1));

        assert !cleanBlocks.containsKey(channel) || cleanBlocks.get(channel).get(number) == null;


        currXaction.addBlock(channel, rv);

        return rv;
    }

    public synchronized void releaseBlock(Block block){
        FileChannel channel = block.getChannel();
        Integer num = block.getBlockNum();

        assert !block.isDirty();
        assert occupiedBlocks.get(channel).get(num) != null;

        Pair<Block, Integer> refCountBlock = occupiedBlocks.get(channel).remove(num);

        if(--refCountBlock.second == 0){
            if(!cleanBlocks.containsKey(channel)){
                cleanBlocks.put(channel, new HashMap<Integer, Block>());
            }

            Map<Integer, Block> blocks = cleanBlocks.get(channel);
            blocks.put(num, block);

            Pair<FileChannel, Integer> entry = new Pair<>(channel, num);
            assert !victimizePool.contains(entry);

            victimizePool.add(entry);

            notify();
        }
    }

    public synchronized void invalidateBlock(Block block) throws IOException{
        FileChannel channel = block.getChannel();
        Integer num = block.getBlockNum();

        assert occupiedBlocks.get(channel).get(num) != null;
        assert cleanBlocks.get(channel) == null || cleanBlocks.get(channel).get(num) == null;

        Pair<Block, Integer> refCountBlock = occupiedBlocks.get(channel).remove(num);

        //only one transaction can own the buffer in X mode
        assert refCountBlock.second == 1;

        availableBuffers.add(block.getBuffer());

        notify();
    }

    //Solely for testing
    public synchronized void reset(){
        this.cleanBlocks = new HashMap<>();
        this.victimizePool = new ArrayList<>();
        int maxBuffNum = config.getMaxBuffNumber();
        this.availableBuffers = new ArrayList<>();
        for(int i = 0; i < maxBuffNum; i++){
            availableBuffers.add(ByteBuffer.allocateDirect(bufferSize));
        }
    }

    public synchronized int getAvailableBuffersNum(){
        return availableBuffers.size();
    }
}
