package file.blockfile;

import buffermanager.BufferManager;
import config.ConfigParameters;
import exception.InvalidBlockExcepxtion;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

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

    //TODO: add blockSize in metadata (only if there is enough time)
    private void initializeMetadata() throws IOException {
        Block header = new Block(0, ByteBuffer.allocateDirect(bufferSize), this, false);
        header.putInt(NUM_OF_BLOCKS_OFFSET, 0);
        header.putInt(FREE_LIST_HEAD_OFFSET, -1);
        header.setBlockNum(0);
        header.writeToFile();
    }

    private int getNumOfBlocks(Block header) throws IOException {
        return header.getInt(NUM_OF_BLOCKS_OFFSET);
    }

    private int getFreeListHead(Block header) throws IOException {
        return header.getInt(FREE_LIST_HEAD_OFFSET);
    }

    private void updateNumOfBlocks(Block header, int newValue) throws IOException {
        header.putInt(NUM_OF_BLOCKS_OFFSET, newValue);
    }

    private void updateFreeListHead(Block header, Block newHead) throws IOException {

        int newFreeListHead = newHead.getBlockNum();
        int oldFreeListHead = getFreeListHead(header);
        header.putInt(FREE_LIST_HEAD_OFFSET, newFreeListHead); //update free list head in metadata block
        newHead.putInt(BLOCK_METADATA_SIZE, oldFreeListHead); //update next block in the new head
        newHead.putByte(ACTIVE_BLOCK_OFFSET, FREE_BLOCK);
    }

    private Block popFreeListHead(Block header) throws IOException {
        Block rv = null;
        int freeListHead = getFreeListHead(header);

        if(freeListHead != -1){
            rv = loadBlock(freeListHead);

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
        else{
            ByteBuffer buff = ByteBuffer.allocateDirect(bufferSize);
            this.channel.read(buff, 0);
        }
    }

    public void close() throws IOException {
        //TODO: invalidate all the blocks in bufferManager
        this.channel.close();
    }

    public FileChannel getChannel() {
        return channel;
    }

    public Block loadBlock(int num) throws IOException {

        return bufManager.getBlock(this, num, false);
    }

    public void commitBlock(Block block) throws IOException, InvalidBlockExcepxtion {

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

        byte active = block.getByte(ACTIVE_BLOCK_OFFSET);

        if(active != ACTIVE_BLOCK)
            throw new InvalidBlockExcepxtion("Trying to dispose a free block");

        updateFreeListHead(loadBlock(0), block);

        block.setDisposed(true);

    }

    public Block allocateNewBlock() throws IOException {
        Block header = loadBlock(0);
        Block rv = popFreeListHead(header);
        boolean newBlock = false;

        if(rv == null){
            int numOfBlocks = getNumOfBlocks(header);
            updateNumOfBlocks(header, ++numOfBlocks);

            rv = bufManager.getBlock(this, numOfBlocks, true);
            newBlock = true;
        }
        rv.putByte(ACTIVE_BLOCK_OFFSET, ACTIVE_BLOCK);

        if(newBlock)
            rv.writeToFile();

        return rv;
    }

    public int getNumOfBlocks() throws IOException {
        return this.getNumOfBlocks(loadBlock(0));
    }

    @Override
    public String toString(){
        StringBuilder rv = new StringBuilder();
        try{
            Block header = loadBlock(0);
            int numOfBlocks = getNumOfBlocks(header);
            int freeListHead = getFreeListHead(header);
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
