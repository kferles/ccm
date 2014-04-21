package file.recordfile;

import config.ConfigParameters;
import exception.InvalidRecordSize;
import file.blockfile.Block;
import file.blockfile.BlockFile;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;

import static util.FileSystemMethods.exists;
import static util.FileSystemMethods.getPath;

public class HeapRecordFile {

    private static final int bufferSize = ConfigParameters.getInstance().getBufferSize();

    private static final int RECORD_SIZE_OFFSET = BlockFile.HEADER_METADATA_SIZE;

    private static final int FIRST_RECORD_OFFSET = BlockFile.BLOCK_METADATA_SIZE;

    private static final int FREE_LIST_BLOCK_OFFSET = RECORD_SIZE_OFFSET + 4;

    private static final int FREE_LIST_REC_OFFSET = FREE_LIST_BLOCK_OFFSET + 4;

    private BlockFile blockFile;

    private final int recordSize;

    private final int recordsPerBlock;

    private void initializeMetadata() throws IOException {
        Block header = blockFile.loadBlock(0);

        header.putInt(RECORD_SIZE_OFFSET, recordSize);
        header.putInt(FREE_LIST_BLOCK_OFFSET, -1);
        header.putInt(FREE_LIST_REC_OFFSET, -1);

        header.writeToFile();
    }

    private RecordPointer getFreeListHead(Block header) throws IOException {
        assert header.getBlockNum() == 0;

        Integer blockNum = header.getInt(FREE_LIST_BLOCK_OFFSET);
        Integer recOffset = header.getInt(FREE_LIST_REC_OFFSET);
        return new RecordPointer(blockNum, recOffset);
    }

    private void updateFreeListHead(Block header, RecordPointer newHead) throws IOException {
        assert header.getBlockNum() == 0;

        header.putInt(FREE_LIST_BLOCK_OFFSET, newHead.getBlockNum());
        header.putInt(FREE_LIST_REC_OFFSET, newHead.getBlockOffset());
    }

    private void updateRecPtr(Block block, int index, RecordPointer recPtr){
        assert index < recordsPerBlock;

        int recOffset = FIRST_RECORD_OFFSET + index*recordSize;
        block.putInt(recOffset, recPtr.getBlockNum());
        block.putInt(recOffset + 4, recPtr.getBlockOffset());
    }

    private RecordPointer getRecPtr(Block block, int index){
        assert index < recordsPerBlock;

        int ptrOffset = FIRST_RECORD_OFFSET + index*recordSize;
        return new RecordPointer(block.getInt(ptrOffset), block.getInt(ptrOffset + 4));
    }

    private void updateRecPtr(Block block, int src, int dest){
        int destRecOffset = FIRST_RECORD_OFFSET + dest*recordSize;
        updateRecPtr(block, src, new RecordPointer(block.getBlockNum(), destRecOffset));
    }

    private Block allocateNewBlock() throws IOException {
        Block newBlock = blockFile.allocateNewBlock();

        int i;
        for(i = 0; i < recordsPerBlock - 1; i++){
            updateRecPtr(newBlock, i, i + 1);
        }

        Block header = blockFile.loadBlock(0);
        RecordPointer freeList = getFreeListHead(header);
        updateRecPtr(newBlock, i, freeList);

        freeList = new RecordPointer(newBlock.getBlockNum(), FIRST_RECORD_OFFSET);
        updateFreeListHead(header, freeList);
        return newBlock;
    }

    public HeapRecordFile(String filename, int recordSize) throws IOException, InvalidRecordSize {
        Path p = getPath(filename);

        if(exists(p))
            throw new FileAlreadyExistsException("Error " + filename + " already exists");

        blockFile = new BlockFile(filename);

        assert recordSize >= 8;
        if(recordSize < 8)
            throw new InvalidRecordSize("Records must be at least four bytes");

        this.recordSize = recordSize;
        this.recordsPerBlock = (bufferSize - BlockFile.BLOCK_METADATA_SIZE)/recordSize;
        initializeMetadata();
    }

    public HeapRecordFile(String filename) throws IOException {
        Path p = getPath(filename);

        if(!exists(p))
            throw new FileNotFoundException("file: " + filename + " does not exist");

        Block header = blockFile.loadBlock(0);

        this.recordSize = header.getInt(RECORD_SIZE_OFFSET);
        this.recordsPerBlock = (bufferSize - BlockFile.BLOCK_METADATA_SIZE)/recordSize;
    }

    public void insertRecord(byte[] rec) throws IOException {
        assert rec.length == recordSize;

        Block header = blockFile.loadBlock(0);
        RecordPointer freeListHead = getFreeListHead(header);
        RecordPointer insertPtr;

        if(freeListHead.getBlockNum() == -1){
            Block newBlock = allocateNewBlock();
            insertPtr = new RecordPointer(newBlock.getBlockNum(), FIRST_RECORD_OFFSET);
        }
        else{
            insertPtr = freeListHead;
        }

        Block toInsert = blockFile.loadBlock(insertPtr.getBlockNum());

        final int recOffset = insertPtr.getBlockOffset();
        RecordPointer newFreeListHead = getRecPtr(toInsert, recOffset);
        updateFreeListHead(header, newFreeListHead);

        for(int i = 0; i < recordSize; ++i)
            toInsert.putByte(recOffset + i, rec[i]);
    }

}
