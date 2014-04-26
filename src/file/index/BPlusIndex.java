package file.index;

import config.ConfigParameters;
import exception.InvalidKeyFactoryException;
import exception.InvalidRecordSize;
import file.blockfile.Block;
import file.blockfile.BlockFile;
import file.record.KeyValueFactory;
import file.record.RecordFactory;
import file.record.SerializableRecord;
import file.recordfile.HeapRecordFile;
import file.recordfile.RecordPointer;
import util.Pair;

import static util.FileSystemMethods.exists;
import static util.FileSystemMethods.getPath;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class BPlusIndex<K extends Comparable<K>, R extends SerializableRecord> {

    private static final int BUFFER_SIZE = ConfigParameters.getInstance().getBufferSize();

    private static final int KEY_SIZE_OFFSET = BlockFile.HEADER_METADATA_SIZE;

    private static final int ROOT_POINTER_OFFSET = KEY_SIZE_OFFSET + 4;

    private static final int NODE_TYPE_OFFSET = BlockFile.BLOCK_METADATA_SIZE;

    private static final byte INNER_NODE = 1;

    private static final byte LEAF_NODE = 2;

    private static final int NODE_POINTERS_NUM_OFFSET = NODE_TYPE_OFFSET + 1;

    private static final int NODE_METADATA_SIZE = NODE_POINTERS_NUM_OFFSET + 4;

    //block number is sufficient
    private static final int INNER_NODE_PTR_SIZE = 4;

    //here we need block number and block offset
    private static final int LEAF_NODE_PTR_SIZE = 8;

    private static final int NEXT_LEAF_NODE_OFFSET = NODE_METADATA_SIZE;

    private HeapRecordFile<R> recordFile;

    private BlockFile indexFile;

    private KeyValueFactory<K> keyValFactory;

    private int keySize;

    private final int pointersPerLeafNode;

    private final int  pointersPerInnerNode;

    private void initializeMetadata() throws IOException {
        ByteBuffer buff = ByteBuffer.allocateDirect(BUFFER_SIZE);
        this.indexFile.getChannel().read(buff, 0);

        Block header = new Block(0, buff, this.indexFile, false);
        header.putInt(KEY_SIZE_OFFSET, this.keySize);
        header.putInt(ROOT_POINTER_OFFSET, -1);

        header.writeToFile();
    }

    private void loadMetadata(String filename) throws IOException, InvalidKeyFactoryException {
        ByteBuffer buff = ByteBuffer.allocateDirect(BUFFER_SIZE);
        this.indexFile.getChannel().read(buff, 0);

        Block header = new Block(0, buff, this.indexFile, false);
        this.keySize = header.getInt(KEY_SIZE_OFFSET);

        if(keySize != keyValFactory.keySize())
            throw new InvalidKeyFactoryException("Invalid key factory for file " + filename);
    }

    private boolean isInnerNode(Block b){
        assert b.getBlockFile() == indexFile;

        return b.getByte(NODE_TYPE_OFFSET) == INNER_NODE;
    }

    private final class InnerNode{

        private Block block;

        public InnerNode(Block block, boolean initialize){
            this.block = block;
            if(initialize){
                block.putByte(NODE_TYPE_OFFSET, INNER_NODE);
                block.putInt(NODE_POINTERS_NUM_OFFSET, 0);
            }
        }

        private void shiftKeysAndPointers(int fromIndex){
            int numOfPointers = getNumOfPointers();

            assert fromIndex <  numOfPointers - 1;

            //shifting keys
            for(int i = numOfPointers - 2; i >= fromIndex; --i){
                K curr = getKey(i);
                setKey(i+1, curr);
            }

            //shifting pointers
            for(int i = numOfPointers - 1; i >= fromIndex + 1; --i){
                int currPtr = getPointer(i);
                setPointer(i, currPtr);
            }
        }

        public int getNumOfPointers(){
            return this.block.getInt(NODE_POINTERS_NUM_OFFSET);
        }

        public void setNumOfPointers(int value){
            this.block.putInt(NODE_POINTERS_NUM_OFFSET, value);
        }

        public int getPointer(int index){
            assert index < getNumOfPointers();
            int pointerOffset = NODE_METADATA_SIZE + pointersPerInnerNode*keySize +
                                index*INNER_NODE_PTR_SIZE;
            return block.getInt(pointerOffset);
        }

        public void setPointer(int index, int value){
            assert  index < getNumOfPointers();

            int pointerOffset = NODE_METADATA_SIZE + pointersPerInnerNode*keySize +
                                index*INNER_NODE_PTR_SIZE;
            this.block.putInt(pointerOffset, value);
        }

        public K getKey(int index){
            assert index < getNumOfPointers() - 1;
            int keyOffset = NODE_METADATA_SIZE + index*keySize;
            byte[] key = new byte[keySize];
            for(int i = 0; i < keySize; ++i)
                key[i] = this.block.getByte(keyOffset + i);
            return keyValFactory.fromByteArray(key);
        }

        public void setKey(int index, K key){
            assert index < getNumOfPointers() - 1;
            int keyOffset = NODE_METADATA_SIZE + index*keySize;
            byte[] keyBytes = keyValFactory.toByteArray(key);

            assert keyBytes.length == keySize;

            for(int i = 0; i < keyBytes.length; ++i)
                this.block.putByte(keyOffset + i, keyBytes[i]);
        }

        public boolean isFull(){
            return getNumOfPointers() == pointersPerInnerNode;
        }

        public int nextPointer(K key){
            int numOfPointers = getNumOfPointers();

            K first = getKey(0), last = getKey(numOfPointers - 2);

            if(key.compareTo(first) < 0)
                return 0;
            else if(key.compareTo(last) >= 0)
                return numOfPointers - 1;

            int imin = 1, imax = numOfPointers - 2;

            while(imax >= imin){

                int imid = (imin + imax) >>> 1;
                K midKey = getKey(imid);

                if(key.compareTo(midKey) < 0){
                    assert imid > 0;
                    K prevMid = getKey(imid - 1);
                    if(key.compareTo(prevMid) >= 0)
                        return imid;
                    else
                        imax = imid - 1;

                }
                else{
                    assert imid < numOfPointers - 2;

                    K nextMid = getKey(imid + 1);
                    if(key.compareTo(nextMid) < 0)
                        return imid + 1;
                    else
                        imin = imid + 1;
                }
            }

            assert false;

            return -1;
        }

        public void insertKey(K key, int gtePointer){
            assert !isFull();

            int insertPosition = nextPointer(key);

            shiftKeysAndPointers(insertPosition);

            setKey(insertPosition, key);
            setPointer(insertPosition, gtePointer);
        }

        public K splitNode(K newKey, int gtePtr, InnerNode newInnerNode){
            assert isFull();

            int insertPosition = nextPointer(newKey);
            List<Pair<K, Integer>> keys = new ArrayList<>();
            for(int i = 0; i < pointersPerInnerNode - 1; ++i){
                if(i == insertPosition)
                    keys.add(new Pair<>(newKey, gtePtr));

                keys.add(new Pair<>(getKey(i), getPointer(i+1)));
            }

            assert keys.size() == pointersPerInnerNode;

            int splitPosition = pointersPerInnerNode >>> 1;

            setNumOfPointers(splitPosition);

            for(int i = 0; i < splitPosition; ++i){
                Pair<K, Integer> currKey = keys.get(i);
                setKey(i, currKey.first);
                setPointer(i+1, currKey.second);
            }

            Pair<K, Integer> median = keys.get(splitPosition);
            newInnerNode.setNumOfPointers(1);
            newInnerNode.setPointer(0, median.second);

            for(int i = splitPosition + 1, newInnerIdx = 0; i < pointersPerInnerNode; ++i, ++newInnerIdx){
                Pair<K, Integer> key = keys.get(i);
                newInnerNode.setNumOfPointers(newInnerIdx + 2);
                newInnerNode.setKey(newInnerIdx, key.first);
                newInnerNode.setPointer(newInnerIdx + 1, key.second);
            }

            return median.first;
        }

        public int getBlockNum(){
            return this.block.getBlockNum();
        }

        @Override
        public String toString(){
            StringBuilder rv = new StringBuilder(Integer.toString(block.getBlockNum()))
                                   .append(":[");
            rv.append(getPointer(0));
            for(int i = 0; i < getNumOfPointers() - 1; ++i){
                rv.append(", ").
                   append("key-").
                   append(i).
                   append(": ").
                   append(getKey(i).toString()).
                   append(", ").
                   append(getPointer(i + 1));
            }
            return rv.append("]").toString();
        }
    }

    private boolean isLeafNode(Block b){
        assert b.getBlockFile() == indexFile;

        return b.getByte(NODE_TYPE_OFFSET) == LEAF_NODE;
    }

    private final class LeafNode {

        private Block block;

        private void shiftKeysAndPointers(int fromIndex){
            int numOfPointers = getNumOfPointers();
            setNumOfPointers(++numOfPointers);

            assert fromIndex < numOfPointers;

            for(int i = numOfPointers - 2; i >= fromIndex; --i){
                Pair<RecordPointer, K> currPtr = getPointer(i);
                setPointer(i + 1, currPtr.second, currPtr.first);
            }
        }

        private int findInsertPosition(K key){
            int numOfPointers = getNumOfPointers();

            assert numOfPointers > 0;

            Pair<RecordPointer, K> first = getPointer(0), last = getPointer(numOfPointers - 1);

            if(key.compareTo(first.second) < 0)
                return 0;
            else if(key.compareTo(last.second) >= 0)
                return numOfPointers;

            int imin = 1, imax = numOfPointers - 1;

            while(imax >= imin){

                int imid = (imin + imax) >>> 1;
                Pair<RecordPointer, K> midKey = getPointer(imid);

                if(key.compareTo(midKey.second) < 0){
                    assert imid > 0;
                    Pair<RecordPointer, K> prevMid = getPointer(imid - 1);
                    if(key.compareTo(prevMid.second) >= 0)
                        return imid;
                    else
                        imax = imid - 1;

                }
                else{
                    assert imid < numOfPointers - 1;

                    Pair<RecordPointer, K> nextMid = getPointer(imid + 1);
                    if(key.compareTo(nextMid.second) < 0)
                        return imid + 1;
                    else
                        imin = imid + 1;
                }
            }

            assert false;

            return -1;
        }

        public LeafNode(Block block, int nextLeaf){
            this.block = block;
            this.block.putByte(NODE_TYPE_OFFSET, LEAF_NODE);
            this.block.putInt(NODE_POINTERS_NUM_OFFSET, 0);
            this.block.putInt(NEXT_LEAF_NODE_OFFSET, nextLeaf);
        }

        public LeafNode(Block block){
            this.block = block;
        }

        public int getNextLeaf(){
            return this.block.getInt(NEXT_LEAF_NODE_OFFSET);
        }

        public void setNextLeaf(int newVal){
            this.block.putInt(NEXT_LEAF_NODE_OFFSET, newVal);
        }

        public int getNumOfPointers(){
            return this.block.getInt(NODE_POINTERS_NUM_OFFSET);
        }

        public void setNumOfPointers(int newVal){
            this.block.putInt(NODE_POINTERS_NUM_OFFSET, newVal);
        }

        public void setPointer(int index, K key, RecordPointer recPtr){
            assert  index < getNumOfPointers();

            int recordOffset = NEXT_LEAF_NODE_OFFSET + 4 + index*(keySize + LEAF_NODE_PTR_SIZE);
            this.block.putInt(recordOffset, recPtr.getBlockNum());
            this.block.putInt(recordOffset + 4, recPtr.getBlockOffset());

            byte[] keyByteArray = keyValFactory.toByteArray(key);
            for(int i = 0; i < keyByteArray.length; ++i)
                this.block.putByte(recordOffset + 8 + i, keyByteArray[i]);
        }

        public Pair<RecordPointer, K> getPointer(int index){
            assert index < getNumOfPointers();

            RecordPointer recPtr;
            int recordOffset = NEXT_LEAF_NODE_OFFSET + 4 + index*(keySize + LEAF_NODE_PTR_SIZE);
            recPtr = new RecordPointer(this.block.getInt(recordOffset), this.block.getInt(recordOffset + 4));

            byte[] keyByteArray = new byte[keySize];
            for(int i = 0; i < keyByteArray.length; ++i)
                keyByteArray[i] = this.block.getByte(recordOffset + 8 + i);

            return new Pair<>(recPtr, keyValFactory.fromByteArray(keyByteArray));
        }

        public void insertPointer(K key, RecordPointer recPtr){
            assert !isFull();

            int numOfPointers = getNumOfPointers();
            int insertPosition = numOfPointers == 0 ? 0 : findInsertPosition(key);

            shiftKeysAndPointers(insertPosition);
            setPointer(insertPosition, key, recPtr);
        }

        public K split(K key, RecordPointer recPtr, LeafNode newLeafNode){
            assert isFull();

            int insertPosition = findInsertPosition(key);

            int splitPosition = (pointersPerLeafNode + 1) >>> 1;

            List<Pair<RecordPointer, K>> allRecs = new ArrayList<>();

            for(int i = 0; i < pointersPerLeafNode; ++i){
                if(i == insertPosition)
                    allRecs.add(new Pair<>(recPtr, key));
                allRecs.add(getPointer(i));
            }

            setNumOfPointers(splitPosition);

            if(insertPosition < splitPosition){
                shiftKeysAndPointers(insertPosition);
                setPointer(insertPosition, key, recPtr);
            }

            K rv = null;
            newLeafNode.setNumOfPointers(pointersPerLeafNode - splitPosition);
            for(int i = 0; i < pointersPerLeafNode - splitPosition; ++i){
                Pair<RecordPointer, K> ptr = allRecs.get(splitPosition + i);
                if(i == 0)
                    rv = ptr.second;
                newLeafNode.setPointer(i, ptr.second, ptr.first);
            }

            return rv;
        }

        public boolean isFull(){
            return getNumOfPointers() == pointersPerLeafNode;
        }

        public int getBlockNum(){
            return this.block.getBlockNum();
        }

        @Override
        public String toString(){
            StringBuilder rv = new StringBuilder(Integer.toString(this.getBlockNum()))
                                   .append(":[");

            int numOfPointers = getNumOfPointers();
            Pair<RecordPointer, K> currPtr = getPointer(0);
            rv.append("{").
               append(currPtr.first.toString()).
               append(", ").
               append(currPtr.second.toString()).
               append('}');

            for(int i = 1; i < numOfPointers; ++i){
                currPtr = getPointer(i);
                rv.append(", {").
                        append(currPtr.first.toString()).
                        append(", ").
                        append(currPtr.second.toString()).
                        append('}');
            }

            rv.append(", ").append(getNextLeaf());

            return  rv.append("]").toString();
        }

    }

    private int getRootBlock(Block header){
        return header.getInt(ROOT_POINTER_OFFSET);
    }

    private LeafNode findLeafNodeForKey(Block root, K key, List<InnerNode> pathFromRoot) throws IOException {
        Block curr = root;
        while(!isLeafNode(curr)){
            assert isInnerNode(curr);
            InnerNode innerCurr = new InnerNode(curr, false);

            if(pathFromRoot != null)
                pathFromRoot.add(innerCurr);

            int nextPtr = innerCurr.nextPointer(key);
            curr = indexFile.loadBlock(nextPtr);
        }

        return new LeafNode(curr);
    }

    public BPlusIndex(String filename, boolean create,
                      RecordFactory<R> recFactory,
                      KeyValueFactory<K> keyValFactory) throws IOException, InvalidRecordSize, InvalidKeyFactoryException {
        Path recPath = getPath(filename);
        String indexFilename = filename + "_b_plus.idx";
        Path indexPath = getPath(indexFilename);

        if(create){
            if(exists(recPath))
                throw new FileAlreadyExistsException("File " + filename + " already exists");
            if(exists(indexPath))
                Files.delete(indexPath);
        }
        else{
            if(!exists(recPath))
                throw new FileNotFoundException("File " + filename + " does not exist");
            if(!exists(indexPath))
                throw new FileNotFoundException("File " + indexFilename + " does not exist");
        }

        this.recordFile = new HeapRecordFile<>(filename, recFactory);
        this.keyValFactory = keyValFactory;

        this.indexFile = new BlockFile(indexFilename);

        this.keySize = this.keyValFactory.keySize();

        int innerNodeAvailableSpace = BUFFER_SIZE - NODE_METADATA_SIZE;
        this.pointersPerInnerNode = (innerNodeAvailableSpace + keySize) / (INNER_NODE_PTR_SIZE + keySize);

        int leafNodeAvailableSpace = BUFFER_SIZE - NODE_METADATA_SIZE - INNER_NODE_PTR_SIZE;
        this.pointersPerLeafNode = leafNodeAvailableSpace / (LEAF_NODE_PTR_SIZE + keySize);
        if(create)
            initializeMetadata();
        else
            loadMetadata(filename);
    }

    public void insert(K key, R record) throws IOException {
        Block header = indexFile.loadBlock(0);

        int rootBlockNum = getRootBlock(header);
        Block root;

        if(rootBlockNum == -1){
            root = indexFile.allocateNewBlock();
        }
        else{
            root = indexFile.loadBlock(rootBlockNum);
        }

        RecordPointer heapPtr = recordFile.insertRecord(record);

        if(rootBlockNum == -1){
            new LeafNode(root, -1);
            header.putInt(ROOT_POINTER_OFFSET, root.getBlockNum());
        }

        List<InnerNode> pathFromRoot = new ArrayList<>();

        LeafNode insertLeaf = findLeafNodeForKey(root, key, pathFromRoot);

        if(!insertLeaf.isFull())
            insertLeaf.insertPointer(key, heapPtr);
        else{
            int leafNext = insertLeaf.getNextLeaf();
            LeafNode newLeaf = new LeafNode(indexFile.allocateNewBlock(), leafNext);

            int newLeafNum = newLeaf.getBlockNum();
            insertLeaf.setNextLeaf(newLeafNum);

            K splitKey = insertLeaf.split(key, heapPtr, newLeaf);
            int gtePtr = newLeafNum;
            int ltRootPtr = -1;
            boolean rootSplit = false;

            if(!pathFromRoot.isEmpty()){
                for(int i = pathFromRoot.size() - 1; i >= 0; --i){
                    InnerNode currNode = pathFromRoot.get(i);
                    if(!currNode.isFull()){
                        currNode.insertKey(splitKey, gtePtr);
                        break;
                    }
                    else{
                        InnerNode newInner = new InnerNode(indexFile.allocateNewBlock(), true);
                        splitKey = currNode.splitNode(splitKey, gtePtr, newInner);
                        gtePtr = newInner.getBlockNum();
                        if(i == 0){
                            rootSplit = true;
                            ltRootPtr = currNode.getBlockNum();
                        }
                    }
                }
            }
            else{
                rootSplit = true;
                ltRootPtr = insertLeaf.getBlockNum();
            }

            if(rootSplit){
                assert ltRootPtr != -1;

                InnerNode newRoot = new InnerNode(indexFile.allocateNewBlock(), true);
                newRoot.setNumOfPointers(2);
                newRoot.setPointer(0, ltRootPtr);
                newRoot.setKey(0, splitKey);
                newRoot.setPointer(1, gtePtr);

                header.putInt(ROOT_POINTER_OFFSET, newRoot.getBlockNum());
            }
        }
    }

    @Override
    public String toString(){
        StringBuilder rv = new StringBuilder();

        try {
            rv.append("Inner Node Degree: ").append(pointersPerInnerNode).append("\nLeaf Node Capacity: ").append(pointersPerLeafNode).append('\n');
            Block header = indexFile.loadBlock(0);
            int rootBlock = getRootBlock(header);

            if(rootBlock != -1){
                List<Pair<Integer, Integer>> fifo = new ArrayList<>();
                fifo.add(new Pair<>(0, rootBlock));


                int currLevel = -1;
                while(!fifo.isEmpty()){
                    Pair<Integer, Integer> curr = fifo.remove(0);
                    int lev = curr.first;
                    Block b = indexFile.loadBlock(curr.second);
                    if(currLevel != lev){
                        rv.append("\nLevel: ").append(lev).append('\n');
                        currLevel = lev;
                    }

                    if(isLeafNode(b)){
                        rv.append(new LeafNode(b).toString()).append(" ");
                    }
                    else{
                        InnerNode innerNode = new InnerNode(b, false);
                        rv.append(innerNode.toString()).append(" ");
                        for(int i = 0; i < innerNode.getNumOfPointers(); ++i)
                            fifo.add(new Pair<>(lev + 1, innerNode.getPointer(i)));
                    }
                }
            }
            else{
                rv.append("Empty tree");
            }

            rv.append("\nRecord File:\n").append(recordFile.toString());

        } catch (IOException e) {
            rv.append(e.getMessage());
        }


        return rv.append('\n').toString();
    }
}
