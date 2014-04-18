package file;

import util.Pair;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;

import static util.FileSystemMethods.exists;
import static util.FileSystemMethods.getPath;

public class HeapRecordFile {

    private static final int RECORD_SIZE_OFFSET = BlockFile.HEADER_METADATA_SIZE;

    private static final int FIRST_RECORD_OFFSET = BlockFile.BLOCK_METADATA_SIZE;

    private static final int FREE_LIST_BLOCK_OFFSET = RECORD_SIZE_OFFSET + 4;

    private static final int FREE_LIST_REC_OFFSET = FREE_LIST_BLOCK_OFFSET + 4;

    private BlockFile blockFile;

    private int recordSize;

    private Pair<Integer, Integer> freeListHead;

    private void initializeMetadata() throws IOException {
        Block header = blockFile.loadBlock(0);

        header.putInt(RECORD_SIZE_OFFSET, recordSize);
        header.putInt(FREE_LIST_BLOCK_OFFSET, -1);
        header.putInt(FREE_LIST_REC_OFFSET, -1);

        blockFile.commitBlock(header);

        freeListHead = new Pair<>(-1, -1);
    }

    public HeapRecordFile(String filename, int recordSize) throws IOException {
        Path p = getPath(filename);

        if(exists(p))
            throw new FileAlreadyExistsException("Error " + filename + " already exists");

        blockFile = new BlockFile(filename);

        this.recordSize = recordSize;

        initializeMetadata();
    }


}
