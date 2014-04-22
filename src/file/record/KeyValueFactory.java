package file.record;

public interface KeyValueFactory<K> {

    public K fromByteArray(byte[] keyVal);

    public int keySize();

}
