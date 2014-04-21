package file.record.sample;

import exception.InvalidRecordSize;
import file.record.RecordFactory;

import java.nio.ByteBuffer;

public class EmployeeFactory implements RecordFactory<EmployeeRecord>{

    @Override
    public EmployeeRecord fromByteArray(byte[] ar) throws InvalidRecordSize {
        if(ar.length != size())
            throw new InvalidRecordSize("Invalid size of byte array for class EmployeeRecord");

        ByteBuffer buff = ByteBuffer.allocateDirect(ar.length);

        buff.put(ar);
        buff.position(0);

        int id = buff.getInt();
        char[] firstName = new char[EmployeeRecord.FIRST_NAME_LENGTH];
        char[] lastName = new char[EmployeeRecord.LAST_NAME_LENGTH];

        for(int i = 0; i < EmployeeRecord.FIRST_NAME_LENGTH; ++i)
            firstName[i] = buff.getChar();

        for(int i = 0; i < EmployeeRecord.LAST_NAME_LENGTH; ++i)
            lastName[i] = buff.getChar();

        return new EmployeeRecord(id, firstName, lastName);
    }

    @Override
    public int size() {
        return EmployeeRecord.size();
    }

}
