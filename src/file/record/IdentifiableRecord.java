package file.record;

public interface IdentifiableRecord<K extends Comparable<K>> {

    public K getKeyValue();

}
