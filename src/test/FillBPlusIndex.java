package test;

import config.ConfigParameters;
import exception.InvalidBlockException;
import exception.InvalidKeyFactoryException;
import exception.InvalidRecordException;
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
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class FillBPlusIndex {

    public static void main(String[] args) throws IOException, InvalidKeyFactoryException, InvalidRecordSize, InvalidBlockException, InvalidRecordException {
        ConfigParameters.getInstance().setMaxBuffNumber(500);
        ConfigParameters.getInstance().setBufferSize(1024);
        String fileName = args[0];
        Path p = FileSystemMethods.getPath(fileName);
        Files.deleteIfExists(p);

        EmployeeFactory employeeFactory = new EmployeeFactory();
        EmployeeKeyValFactory employeeKeyValFactory = new EmployeeKeyValFactory();
        BPlusIndex<Integer, EmployeeRecord> bPlusIndex = new BPlusIndex<>(fileName, true, employeeFactory, employeeKeyValFactory);

        Xaction t1 = new Xaction();
        Random r = new Random();
        Set<Integer> genIds = new HashSet<>();

        System.out.println("Inserting phase...");
        for(int i = 0;  i < 3000000; ++i){
            Integer id = r.nextInt();
            while(genIds.contains(id))
                id = r.nextInt();
            genIds.add(id);

            t1.begin();
            bPlusIndex.insert(id, new EmployeeRecord(id, "FirstName", "LastName"));
            t1.commit();
            t1.end();
        }
    }
}
