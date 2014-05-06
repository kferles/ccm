package test;

import config.ConfigParameters;
import exception.InvalidBlockExcepxtion;
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

public class TestBPlusUpdateAndRange {


    public static void main(String[] args) throws IOException, InvalidKeyFactoryException, InvalidRecordSize, InvalidBlockExcepxtion, InvalidRecordException {
        ConfigParameters.getInstance().setMaxBuffNumber(500);
        //Making buffer size very small, only one record fits in a buffer.
        ConfigParameters.getInstance().setBufferSize(150);
        String fileName = "test_file_up_range";
        Path p = FileSystemMethods.getPath(fileName);
        Files.deleteIfExists(p);

        EmployeeFactory employeeFactory = new EmployeeFactory();
        EmployeeKeyValFactory employeeKeyValFactory = new EmployeeKeyValFactory();
        BPlusIndex<Integer, EmployeeRecord> bPlusIndex = new BPlusIndex<>(fileName, true, employeeFactory, employeeKeyValFactory);

        Xaction t1 = new Xaction();

        int id = 0;
        System.out.println("Inserting phase...");
        for(int i = 0;  i < 300; ++i, id += 5){
            t1.begin();
            bPlusIndex.insert(id, new EmployeeRecord(id, "Kostas" + id, "Ferles" + id));
            t1.commit();
            t1.end();
        }

        System.out.println("Updating phase");
        t1.begin();
        bPlusIndex.update(new EmployeeRecord(10, "Kostas", "Ferles"));
        bPlusIndex.update(new EmployeeRecord(100, "Kostas", "Ferles"));

        bPlusIndex.update(new EmployeeRecord(5, "Kostas", "Ferles"));
        bPlusIndex.update(new EmployeeRecord(55, "Kostas", "Ferles"));
        t1.commit();
        t1.end();

        System.out.println("Searching phase");
        t1.begin();
        EmployeeRecord rec = bPlusIndex.get(10);
        assert rec.getId() == 10 && rec.getFirstName().equals("Kostas")
               && rec.getLastName().equals("Ferles");
        rec = bPlusIndex.get(100);
        assert rec.getId() == 100 && rec.getFirstName().equals("Kostas")
                && rec.getLastName().equals("Ferles");
        rec = bPlusIndex.get(5);
        assert rec.getId() == 5 && rec.getFirstName().equals("Kostas")
                && rec.getLastName().equals("Ferles");
        rec = bPlusIndex.get(55);
        assert rec.getId() == 55 && rec.getFirstName().equals("Kostas")
                && rec.getLastName().equals("Ferles");
        t1.commit();
        t1.end();

        System.out.println("Ranges");
        t1.begin();
        System.out.println(bPlusIndex.recordsInRange(5, 10));
        System.out.println(bPlusIndex.recordsInRange(5, 100));
        System.out.println(bPlusIndex.recordsInRange(3, 13));
        System.out.println(bPlusIndex.recordsInRange(3, 258));
        t1.commit();
        t1.end();
    }
}
