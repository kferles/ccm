package file.record.sample;

import exception.InvalidRecordSize;
import file.record.IdentifiableRecord;
import file.record.SerializableRecord;

import java.nio.ByteBuffer;

public class EmployeeRecord implements SerializableRecord,
                                       IdentifiableRecord<Integer>,
                                       Comparable<EmployeeRecord>{

    static final int FIRST_NAME_LENGTH = 30;

    static final int LAST_NAME_LENGTH = 30;

    public static int size(){
        return 4 + 2*EmployeeRecord.FIRST_NAME_LENGTH + 2*EmployeeRecord.LAST_NAME_LENGTH;
    }

    private int id;

    private char[] firstName = new char[FIRST_NAME_LENGTH];

    private char[] lastName = new char[LAST_NAME_LENGTH];

    private byte[] idToByteArray(){
        return ByteBuffer.allocate(4).putInt(id).array();
    }

    private byte[] charArrayToByteArray(char[] src){
        ByteBuffer buff = ByteBuffer.allocate(2 * src.length);
        buff.asCharBuffer().put(src);
        return buff.array();
    }

    private void moveFromByteArray(byte[] dest, int fromIndex, byte[] src){
        assert dest.length - fromIndex >= src.length;

        System.arraycopy(src, 0, dest, fromIndex, src.length);
    }

    private static void copyFromString(char[] dest, String src){
        int strLength = src.length();
        for(int i = 0; i < strLength; ++i)
            if(i < strLength)
                dest[i] = src.charAt(i);
            else
                dest[i] = '\0';
    }

    public EmployeeRecord(int id, char[] firstName, char[] lastName) throws InvalidRecordSize {
        if(firstName.length != FIRST_NAME_LENGTH)
            throw new InvalidRecordSize("Invalid first name length for class Employee");
        if(lastName.length != LAST_NAME_LENGTH)
            throw new InvalidRecordSize("Invalid last name length for class Employee");
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    public EmployeeRecord(int id, String firstName, String lastName){
        this.id = id;
        copyFromString(this.firstName, firstName);
        copyFromString(this.lastName, lastName);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getFirstName() {
        return new String(firstName);
    }

    public void setFirstName(String firstName) {
        copyFromString(this.firstName, firstName);
    }

    public String getLastName() {
        return new String(lastName);
    }

    public void setLastName(String lastName) {
        copyFromString(this.lastName, lastName);
    }

    @Override
    public byte[] toByteArray() {
        byte[] rv = new byte[size()];

        moveFromByteArray(rv, 0, idToByteArray());
        moveFromByteArray(rv, 4, charArrayToByteArray(firstName));
        //WARNING: here it is crucial to multiply FIRST_NAME_LENGTH with 2
        //         otherwise first and last names fields get mixed during
        //         de-serialization.
        moveFromByteArray(rv, 4 + 2*FIRST_NAME_LENGTH, charArrayToByteArray(lastName));
        return rv;
    }

    @Override
    public String toString(){
        return "[" + id + ", " + new String(firstName) + ", "
                   + new String(lastName) + "]";
    }

    @Override
    public Integer getKeyValue() {
        return this.id;
    }

    @Override
    public byte[] keyToByteArray() {
        return idToByteArray();
    }

    @Override
    public int compareTo(EmployeeRecord o) {
        return Integer.compare(this.id, o.id);
    }
}
