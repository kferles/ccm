package test;

import config.ConfigParameters;
import exception.InvalidBlockExcepxtion;
import exception.InvalidKeyFactoryException;
import exception.InvalidRecordSize;
import file.index.BPlusIndex;
import file.record.sample.EmployeeFactory;
import file.record.sample.EmployeeKeyValFactory;
import file.record.sample.EmployeeRecord;
import util.FileSystemMethods;
import xaction.Xaction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class TestBPlusIndex {

    public static void main(String[] args) throws IOException, InvalidRecordSize, InvalidBlockExcepxtion, InvalidKeyFactoryException {

        //Making buffer size very small, only one record fits in a buffer.
        ConfigParameters.getInstance().setBufferSize(150);
        String fileName = "test_file";
        Path p = FileSystemMethods.getPath(fileName);
        Files.deleteIfExists(p);

        EmployeeFactory employeeFactory = new EmployeeFactory();
        EmployeeKeyValFactory employeeKeyValFactory = new EmployeeKeyValFactory();
        BPlusIndex<Integer, EmployeeRecord> bPlusIndex = new BPlusIndex<>(fileName, true, employeeFactory, employeeKeyValFactory);
        int currId = 0;
        Xaction t1 = new Xaction();
        t1.begin();
        System.out.println(bPlusIndex.toString());
        t1.commit();
        t1.end();

        t1.begin();
        for(int i = 0; i < 11; ++i)
            bPlusIndex.insert(currId++, new EmployeeRecord(currId-1, "Kostas", "Ferles"));
        //Checking leaf split
        bPlusIndex.insert(currId, new EmployeeRecord(currId, "Kostas", "Ferles"));
        System.out.println(bPlusIndex.toString());
        t1.commit();
        t1.end();

        Files.delete(p);
        currId = 1;
        bPlusIndex = new BPlusIndex<>(fileName, true, employeeFactory, employeeKeyValFactory);
        t1.begin();
        //Checking key shifting
        for(int i = 0; i < 10; i += 2, currId += 2)
            bPlusIndex.insert(currId, new EmployeeRecord(currId, "Kostas", "Ferles"));
        currId = 2;
        for(int i = 1; i < 10; i += 2, currId += 2)
            bPlusIndex.insert(currId, new EmployeeRecord(currId, "Kostas", "Ferles"));
        //shift it all the way
        bPlusIndex.insert(0, new EmployeeRecord(0, "Kostas", "Ferles"));
        System.out.println(bPlusIndex.toString());
        t1.commit();
        t1.end();
    }

}
