package test;

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

public class TestBplusIndex {

    public static void main(String[] args) throws IOException, InvalidRecordSize, InvalidBlockExcepxtion, InvalidKeyFactoryException {

        String fileName = "test_file";
        Path p = FileSystemMethods.getPath(fileName);
        Files.deleteIfExists(p);

        BPlusIndex<Integer, EmployeeRecord> bPlusIndex = new BPlusIndex<>(fileName, true, new EmployeeFactory(), new EmployeeKeyValFactory());
        int currId = 0;
        Xaction t1 = new Xaction();
        t1.begin();
        System.out.println(bPlusIndex.toString());
        t1.commit();
        t1.end();

        t1.begin();
        for(int i = 0; i < 11; ++i){
            System.out.println(i);
            bPlusIndex.insert(new EmployeeRecord(currId++, "Kostas", "Ferles"));
        }
        bPlusIndex.insert(new EmployeeRecord(currId++, "Kostas", "Ferles"));
        System.out.println(bPlusIndex.toString());
        t1.commit();
        t1.end();
    }

}