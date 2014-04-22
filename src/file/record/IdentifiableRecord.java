package file.record;

public interface IdentifiableRecord<K> {

    public K getKeyValue();

    public byte[] keyToByteArray();

}
