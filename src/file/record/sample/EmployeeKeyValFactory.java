package file.record.sample;

import file.record.KeyValueFactory;

import java.nio.ByteBuffer;

public class EmployeeKeyValFactory implements KeyValueFactory<Integer> {

    @Override
    public Integer fromByteArray(byte[] keyVal) {
        assert keyVal.length == keySize();
        ByteBuffer buffer = ByteBuffer.allocate(keySize());
        buffer.put(keyVal);
        return buffer.getInt(0);
    }

    @Override
    public int keySize() {
        return 4;
    }
}
