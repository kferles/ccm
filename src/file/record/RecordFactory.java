package file.record;

import exception.InvalidRecordSize;

public interface RecordFactory<T extends SerializableRecord> {

    public T fromByteArray(byte[] src) throws InvalidRecordSize;

    public int size();

}
