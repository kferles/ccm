package test;

import exception.InvalidBlockExcepxtion;
import exception.InvalidRecordSize;
import file.record.sample.EmployeeFactory;
import file.record.sample.EmployeeRecord;
import file.recordfile.HeapRecordFile;
import file.recordfile.RecordPointer;
import xaction.Xaction;

import java.io.IOException;
import java.util.ArrayList;

public class TestHeapFile {

    private static final String heapFilename = "test_heap_file";

    private static final EmployeeRecord dummyRecord = new EmployeeRecord(1, "Kostas", "Ferles");

    public static void main(String[] args) throws IOException, InvalidRecordSize, InvalidBlockExcepxtion {
        Xaction t1 = new Xaction();
        HeapRecordFile<EmployeeRecord> file = new HeapRecordFile<>(heapFilename,
                                                                   new EmployeeFactory());
        t1.begin();
        System.out.println(file.toString());
        t1.commit();
        t1.end();

        file = new HeapRecordFile<>(heapFilename,
                                    new EmployeeFactory());

        t1.begin();
        System.out.println(file.toString());
        t1.commit();
        t1.end();

        t1.begin();
        ArrayList<RecordPointer> in = new ArrayList<>();
        for(int i = 0; i < 8; ++i){
            in.add(file.insertRecord(dummyRecord));
        }
        t1.commit();
        t1.end();

        t1.begin();
        System.out.println(file.toString());
        for(int i = 0; i < 8; ++i){
            file.deleteRecord(in.get(i));
        }
        t1.commit();
        t1.end();

        t1.begin();
        System.out.println(file.toString());
        t1.commit();
        t1.end();

        in.clear();

        t1.begin();
        for(int i = 0; i < 2*8; ++i){
            in.add(file.insertRecord(dummyRecord));
        }
        t1.commit();
        t1.end();

        t1.begin();
        System.out.println(file.toString());
        for(int i = 0; i < 2*8; i+=2){
            file.deleteRecord(in.get(i));
        }
        t1.commit();
        t1.end();

        t1.begin();
        System.out.println(file.toString());
        t1.commit();
        t1.end();
    }
}
