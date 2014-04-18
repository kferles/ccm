package file;

import config.ConfigParameters;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by kostas on 4/16/14.
 */
public class Block {

    private static final int bufferSize = ConfigParameters.getInstance().getBufferSize();

    private FileChannel channel;

    private int blockNum;

    private ByteBuffer buffer;

    private boolean dirty = false;

    public Block(int blockNum, ByteBuffer buffer, FileChannel channel){
        this.blockNum = blockNum;
        this.buffer = buffer;
        this.channel = channel;
    }

    public int getBlockNum() {
        return blockNum;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void setBlockNum(int blockNum) {
        this.dirty = true;
        this.blockNum = blockNum;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void putInt(int index, int value){
        this.dirty = true;
        this.buffer.putInt(index, value);
    }

    public int getInt(int index){
        return this.buffer.getInt(index);
    }

    public void putByte(int index, byte value){
        this.dirty = true;
        this.buffer.put(index, value);
    }

    public byte getByte(int index){
        return this.buffer.get(index);
    }

    public void writeToFile() throws IOException {
        assert this.blockNum > 0;
        assert this.dirty == true;

        buffer.position(0);
        channel.position(bufferSize*blockNum);
        while (buffer.hasRemaining())
            channel.write(buffer);
        channel.force(true);
        this.dirty = false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Block block = (Block) o;

        if (blockNum != block.blockNum) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return blockNum;
    }

    @Override
    public String toString(){
        StringBuilder rv = new StringBuilder("#Block-").append(blockNum).append(":\n");
        for(int i = 0; i < ConfigParameters.getInstance().getBufferSize(); i++)
            rv.append(buffer.get(i));
        return rv.toString();
    }
}
