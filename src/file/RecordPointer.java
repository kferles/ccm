package file;

import util.Pair;

public class RecordPointer {

    private Pair<Integer, Integer> pointer;

    public RecordPointer(int blockNum, int blockOffset){
        pointer = new Pair<>(blockNum, blockOffset);
    }

    public int getBlockNum(){
        return pointer.first;
    }

    public void setBlockNum(int blockNum){
        this.pointer.first = blockNum;
    }

    public int getBlockOffset(){
        return pointer.second;
    }

    public void setBlockOffset(int offset){
        this.pointer.second = offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RecordPointer that = (RecordPointer) o;

        if (!pointer.equals(that.pointer)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return pointer.hashCode();
    }
}
