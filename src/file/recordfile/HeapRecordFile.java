package file.recordfile;

import config.ConfigParameters;
import exception.InvalidRecordSize;
import file.blockfile.Block;
import file.blockfile.BlockFile;
import file.record.RecordFactory;
import file.record.SerializableRecord;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static util.FileSystemMethods.exists;
import static util.FileSystemMethods.getPath;

public class HeapRecordFile<R extends SerializableRecord> {

    private static final int bufferSize = ConfigParameters.getInstance().getBufferSize();

    private static final int RECORD_SIZE_OFFSET = BlockFile.HEADER_METADATA_SIZE;

    private static final int FIRST_RECORD_OFFSET = BlockFile.BLOCK_METADATA_SIZE;

    private static final int FREE_LIST_BLOCK_OFFSET = RECORD_SIZE_OFFSET + 4;

    private static final int FREE_LIST_REC_OFFSET = FREE_LIST_BLOCK_OFFSET + 4;

    private RecordFactory<R> recFactory;

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
        assert src < recordsPerBlock && dest < recordsPerBlock;
        updateRecPtr(block, src, new RecordPointer(block.getBlockNum(), dest));
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

    public HeapRecordFile(String filename, RecordFactory<R> recFactory) throws IOException, InvalidRecordSize {
        Path p = getPath(filename);

        this.recFactory = recFactory;
        blockFile = new BlockFile(filename);

        if(!exists(p)){
            recordSize = recFactory.size();
            assert recordSize >= 8;
            if(recordSize < 8)
                throw new InvalidRecordSize("Records must be at least four bytes");

            this.recordsPerBlock = (bufferSize - BlockFile.BLOCK_METADATA_SIZE)/recordSize;
            initializeMetadata();
        }
        else{
            Block header = blockFile.loadBlock(0);

            this.recordSize = header.getInt(RECORD_SIZE_OFFSET);

            if(recFactory.size() != recordSize)
                throw new InvalidRecordSize("Size in heap file " + filename
                                            + " does not match size of given class");

            this.recordsPerBlock = (bufferSize - BlockFile.BLOCK_METADATA_SIZE)/recordSize;
        }
    }

    public RecordPointer insertRecord(R record) throws IOException {

        byte[] rec = record.toByteArray();

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

        return insertPtr;
    }

    public void deleteRecord(RecordPointer recPtr) throws IOException {
        Block header = blockFile.loadBlock(0);
        RecordPointer freeListHead = getFreeListHead(header);

        Block deleteFrom = blockFile.loadBlock(recPtr.getBlockNum());
        updateRecPtr(deleteFrom, recPtr.getBlockOffset(), freeListHead);

        updateFreeListHead(header, recPtr);
    }

    public R getRecord(RecordPointer recPtr) throws IOException, InvalidRecordSize {
        Block block = blockFile.loadBlock(recPtr.getBlockNum());
        byte[] rec = new byte[recordSize];
        int recIndex = recPtr.getBlockOffset();
        for(int i = 0; i < recordSize; ++i){
            rec[i] = block.getByte(FIRST_RECORD_OFFSET + recIndex*recordSize + i);
        }

        return recFactory.fromByteArray(rec);
    }

    @Override
    public String toString(){
        StringBuilder rv = new StringBuilder();
        List<RecordPointer> freeList = new ArrayList<>();
        try {
            Block header = blockFile.loadBlock(0);
            RecordPointer curr = getFreeListHead(header);
            while(curr.getBlockNum() != -1){
                freeList.add(curr);
                Block nextBlock = blockFile.loadBlock(curr.getBlockNum());
                curr = getRecPtr(nextBlock, curr.getBlockOffset());
            }
            rv.append("Free List ").append(freeList.toString()).append('\n');

            for(int i = 0, numOfBlocks = blockFile.getNumOfBlocks(); i < numOfBlocks; ++i){
                rv.append("Block-").append(i+1).append("{\n");
                RecordPointer ptr = new RecordPointer(i+1, -1);
                for(int j = 0; j < recordsPerBlock; ++j){
                    ptr.setBlockOffset(j);
                    if(!freeList.contains(ptr)){
                        rv.append(getRecord(ptr).toString()).append("\n");
                    }
                }
                rv.append("}\n");
            }
        } catch (IOException | InvalidRecordSize e) {
            rv.append(e.getMessage());
        }

        return rv.toString();
    }

}
