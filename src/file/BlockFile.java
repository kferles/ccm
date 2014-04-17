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

    //TODO: check when loading a block that it's still valid, same at dispose

    private static final int bufferSize = ConfigParameters.getInstance().getBufferSize();

    private static final BufferManager bufManager = BufferManager.getInstance();

    private static final int NUM_OF_BLOCKS_OFFSET = 0;

    private static final int FREE_LIST_HEAD_OFFSET = 4;

    private static final int ACTIVE_BLOCK_OFFSET = 0;

    private static final int HEADER_METADATA_SIZE = 8;

    private static final int BLOCK_METADATA_SIZE = 1;

    private static final byte ACTIVE_BLOCK = 1;

    private static final byte FREE_BLOCK = 0;

    private FileChannel channel;

    private int numOfBlocks = 0;

    private int freeListHead = -1;

    private Map<Integer, Block> loadedBlocks = new HashMap<>();

    private void initializeMetadata() throws IOException {
        ByteBuffer buf = bufManager.getBuffer();
        buf.putInt(0);
        buf.putInt(-1);
        writeBufferToFile(channel, buf, 0);
    }

    private void loadMetadata() throws IOException {
        Block header = loadBlock(0);

        this.numOfBlocks = header.getInt(NUM_OF_BLOCKS_OFFSET);
        this.freeListHead = header.getInt(FREE_LIST_HEAD_OFFSET);
    }

    private void updateNumOfBlocks(int newValue) throws IOException {
        Block header = loadBlock(0);
        header.putInt(NUM_OF_BLOCKS_OFFSET, newValue);
        writeBufferToFile(channel, header.getBuffer(), 0);
        numOfBlocks = newValue;
        commitBlock(header);
    }

    private void updateFreeListHead(Block newHead) throws IOException {
        Block header = loadBlock(0);

        int newFreeListHead = newHead.getBlockNum();
        header.putInt(FREE_LIST_HEAD_OFFSET, newFreeListHead); //update free list head in metadata block
        newHead.putInt(BLOCK_METADATA_SIZE, freeListHead); //update next block in the new head
        freeListHead = newFreeListHead;

        commitBlock(header);
    }

    private Block popFreeListHead() throws IOException {
        Block rv = null;
        if(freeListHead != -1){
            rv = loadBlock(freeListHead);

            Block header = loadBlock(0);

            int newFreeListHead = rv.getInt(BLOCK_METADATA_SIZE);

            header.putInt(FREE_LIST_HEAD_OFFSET, newFreeListHead); //update free list head
            freeListHead = newFreeListHead;

            commitBlock(header);
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

        Block rv = new Block(num, buffer);
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
            updateNumOfBlocks(numOfBlocks + 1);
            block.setBlockNum(numOfBlocks);
            block.setDirty(true); //just to be sure
        }
        ByteBuffer buffer = block.getBuffer();

        if(block.isDirty()){
            writeBufferToFile(channel, buffer, block.getBlockNum());
            block.setDirty(false);
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
            block.putByte(ACTIVE_BLOCK_OFFSET, FREE_BLOCK);
            commitBlock(block);
        }

        bufManager.releaseBuffer(block.getBuffer());
    }

    public Block allocateNewBlock() throws IOException {
        Block rv = popFreeListHead();

        if(rv == null){
           ByteBuffer buffer = bufManager.getBuffer();
           rv = new Block(-1, buffer);
        }
        rv.putByte(ACTIVE_BLOCK_OFFSET, ACTIVE_BLOCK);

        return rv;
    }

    public static int getActiveBlockOffset() {
        return ACTIVE_BLOCK_OFFSET;
    }

    public static int getHeaderMetadataSize() {
        return HEADER_METADATA_SIZE;
    }

    public static int getBlockMetadataSize() {
        return BLOCK_METADATA_SIZE;
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
