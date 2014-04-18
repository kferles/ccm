package file;

import buffermanager.BufferManager;
import config.ConfigParameters;
import exception.InvalidBlockExcepxtion;
import xaction.Xaction;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static util.FileSystemMethods.*;

/**
 * Created by kostas on 4/15/14.
 */
public final class BlockFile {

    public static final int HEADER_METADATA_SIZE = 8;

    public static final int BLOCK_METADATA_SIZE = 1;

    private static final int bufferSize = ConfigParameters.getInstance().getBufferSize();

    private static final BufferManager bufManager = BufferManager.getInstance();

    private static final int NUM_OF_BLOCKS_OFFSET = 0;

    private static final int FREE_LIST_HEAD_OFFSET = 4;

    private static final int ACTIVE_BLOCK_OFFSET = 0;

    private static final byte ACTIVE_BLOCK = 1;

    private static final byte FREE_BLOCK = 0;

    private FileChannel channel;

    private Map<Integer, Block> loadedBlocks = new HashMap<>();

    private void initializeMetadata() throws IOException {
        Block header = new Block(0, ByteBuffer.allocateDirect(bufferSize), this);
        header.putInt(NUM_OF_BLOCKS_OFFSET, 0);
        header.putInt(FREE_LIST_HEAD_OFFSET, -1);
        header.setBlockNum(0);
        header.writeToFile();
    }

    private int getNumOfBlocks() throws IOException {
        Block header = loadBlock(0);
        return header.getInt(NUM_OF_BLOCKS_OFFSET);
    }

    private int getFreeListHead() throws IOException {
        Block header = loadBlock(0);
        return header.getInt(FREE_LIST_HEAD_OFFSET);
    }

    private void updateNumOfBlocks(int newValue) throws IOException {
        Block header = loadBlock(0);
        header.putInt(NUM_OF_BLOCKS_OFFSET, newValue);
    }

    private void updateFreeListHead(Block newHead) throws IOException {
        Block header = loadBlock(0);

        int newFreeListHead = newHead.getBlockNum();
        int oldFreeListHead = getFreeListHead();
        header.putInt(FREE_LIST_HEAD_OFFSET, newFreeListHead); //update free list head in metadata block
        newHead.putInt(BLOCK_METADATA_SIZE, oldFreeListHead); //update next block in the new head
        newHead.putByte(ACTIVE_BLOCK_OFFSET, FREE_BLOCK);
    }

    private Block popFreeListHead() throws IOException {
        Block rv = null;
        int freeListHead = getFreeListHead();

        if(freeListHead != -1){
            rv = loadBlock(freeListHead);

            Block header = loadBlock(0);

            int newFreeListHead = rv.getInt(BLOCK_METADATA_SIZE);

            header.putInt(FREE_LIST_HEAD_OFFSET, newFreeListHead); //update free list head
        }
        return rv;
    }

    public BlockFile(String filename) throws IOException {
        boolean initialize = false;
        Path p = getPath(filename);

        if(!exists(p)){
            createFile(p);
            initialize = true;
        }

        RandomAccessFile file = new RandomAccessFile(filename, "rw");
        this.channel = file.getChannel();

        if(initialize)
            initializeMetadata();
    }

    public void close() throws IOException {
        //TODO: invalidate all the blocks in bufferManager
        this.channel.close();
    }

    public FileChannel getChannel() {
        return channel;
    }

    public Block loadBlock(int num) throws IOException {

        assert num == 0 || num <= getNumOfBlocks();

        if(loadedBlocks.containsKey(num)){
            Block rv = loadedBlocks.get(num);
            assert Xaction.getExecutingXaction().contains(rv);
            return rv;
        }

        Block rv = bufManager.getBlock(this, num, false);
        loadedBlocks.put(num, rv);

        return rv;
    }

    public void commitBlock(Block block) throws IOException {

        int blockNum = block.getBlockNum();

        if(blockNum != -1){
            assert loadedBlocks.containsKey(blockNum);
            loadedBlocks.remove(blockNum);
        }
        else{
            if(block.isDisposed()){
                bufManager.invalidateBlock(block);
                return;
            }
            int numOfBlocks = getNumOfBlocks();
            updateNumOfBlocks(++numOfBlocks);
            block.setBlockNum(numOfBlocks);
        }

        if(block.isDirty()){
            block.writeToFile();
        }

        //REVIEW: Is this a good place to call release/invalidateBlock methods?
        if(!block.isDisposed()){
            bufManager.releaseBlock(block);
        }
        else{
            bufManager.invalidateBlock(block);
        }
    }

    public void disposeBlock(Block block) throws IOException, InvalidBlockExcepxtion {

        int blockNum = block.getBlockNum();

        assert blockNum > 0 || blockNum == -1;

        byte active = block.getByte(ACTIVE_BLOCK_OFFSET);

        if(active != ACTIVE_BLOCK)
            throw new InvalidBlockExcepxtion("Trying to dispose a free block");

        if(blockNum != -1){
            assert loadedBlocks.containsKey(blockNum);
            updateFreeListHead(block);
            System.out.println(block.isDirty());
        }

        block.setDisposed(true);

    }

    public Block allocateNewBlock() throws IOException {
        Block rv = popFreeListHead();

        if(rv == null){
           rv = bufManager.getBlock(this, -1, true);
        }
        rv.putByte(ACTIVE_BLOCK_OFFSET, ACTIVE_BLOCK);

        return rv;
    }

    @Override
    public String toString(){
        StringBuilder rv = new StringBuilder();
        try{
            int numOfBlocks = getNumOfBlocks();
            int freeListHead = getFreeListHead();
            rv.append("Num of blocks = ").append(numOfBlocks + 1).append('\n');
            rv.append("Free List Head = ").append(freeListHead).append('\n');
            for(int i = 0; i <= numOfBlocks; ++i)
                rv.append(loadBlock(i).toString()).append("\n");
        }
        catch (IOException ex){
            rv.append(ex.getMessage());
        }
        return rv.toString();
    }

}
