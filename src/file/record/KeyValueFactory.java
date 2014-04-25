package file.record;

public interface KeyValueFactory<K> {

    public K fromByteArray(byte[] keyVal);

    public byte[] toByteArray(K key);

    public int keySize();

}
