package file;

import buffermanager.BufferManager;
import config.ConfigParameters;
import exception.InvalidBlockExcepxtion;

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
public class BlockFile {

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

    private int numOfBlocks = 0;

    private int freeListHead = -1;

    private Map<Integer, Block> loadedBlocks = new HashMap<>();

    private void initializeMetadata() throws IOException {
        Block header = allocateNewBlock();
        header.putInt(NUM_OF_BLOCKS_OFFSET, 0);
        header.putInt(FREE_LIST_HEAD_OFFSET, -1);
        header.setBlockNum(0);
        header.writeToFile();
    }

    private void loadMetadata() throws IOException {
        Block header = loadBlock(0);

        this.numOfBlocks = header.getInt(NUM_OF_BLOCKS_OFFSET);
        this.freeListHead = header.getInt(FREE_LIST_HEAD_OFFSET);
    }

    private void updateNumOfBlocks(int newValue) throws IOException {
        Block header = loadBlock(0);
        header.putInt(NUM_OF_BLOCKS_OFFSET, newValue);
        commitBlock(header);
        numOfBlocks = newValue;
    }

    private void updateFreeListHead(Block newHead) throws IOException {
        Block header = loadBlock(0);

        int newFreeListHead = newHead.getBlockNum();
        header.putInt(FREE_LIST_HEAD_OFFSET, newFreeListHead); //update free list head in metadata block
        newHead.putInt(BLOCK_METADATA_SIZE, freeListHead); //update next block in the new head
        newHead.putByte(ACTIVE_BLOCK_OFFSET, FREE_BLOCK);

        commitBlock(newHead);
        commitBlock(header);

        freeListHead = newFreeListHead;
    }

    private Block popFreeListHead() throws IOException {
        Block rv = null;
        if(freeListHead != -1){
            rv = loadBlock(freeListHead);

            Block header = loadBlock(0);

            int newFreeListHead = rv.getInt(BLOCK_METADATA_SIZE);

            header.putInt(FREE_LIST_HEAD_OFFSET, newFreeListHead); //update free list head
            commitBlock(header);

            freeListHead = newFreeListHead;
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
        else
            loadMetadata();
    }

    public Block loadBlock(int num) throws IOException {

        assert num <= numOfBlocks;

        if(loadedBlocks.containsKey(num)){
            return loadedBlocks.get(num);
        }

        ByteBuffer buffer = bufManager.getBuffer();
        channel.read(buffer, num*bufferSize);

        Block rv = new Block(num, buffer, channel);
        loadedBlocks.put(num, rv);

        return rv;
    }

    public final void commitBlock(Block block) throws IOException {

        int blockNum = block.getBlockNum();

        if(blockNum != -1){
            assert loadedBlocks.containsKey(blockNum);
            loadedBlocks.remove(blockNum);
        }
        else{
            updateNumOfBlocks(numOfBlocks + 1);
            block.setBlockNum(numOfBlocks);
        }
        ByteBuffer buffer = block.getBuffer();

        if(block.isDirty()){
            block.writeToFile();
        }

        bufManager.releaseBuffer(buffer);
    }

    public void disposeBlock(Block block) throws IOException, InvalidBlockExcepxtion {

        int blockNum = block.getBlockNum();

        assert blockNum > 0;

        byte active = block.getByte(ACTIVE_BLOCK_OFFSET);

        if(active != ACTIVE_BLOCK)
            throw new InvalidBlockExcepxtion("Trying to dispose a free block");

        if(blockNum != -1){
            assert loadedBlocks.containsKey(blockNum);
            updateFreeListHead(block);
        }

        bufManager.releaseBuffer(block.getBuffer());
    }

    public final Block allocateNewBlock() throws IOException {
        Block rv = popFreeListHead();

        if(rv == null){
           ByteBuffer buffer = bufManager.getBuffer();
           rv = new Block(-1, buffer, channel);
        }
        rv.putByte(ACTIVE_BLOCK_OFFSET, ACTIVE_BLOCK);

        return rv;
    }

    public int getNumOfBlocks(){
        return this.numOfBlocks;
    }

    @Override
    public String toString(){
        StringBuilder rv = new StringBuilder();
        try{
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
