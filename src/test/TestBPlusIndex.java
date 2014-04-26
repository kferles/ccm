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
import java.util.*;

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

        System.out.println("Testing leaf split...");
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
        System.out.println("Testing key shifting");
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

        System.out.println("Testing Inner Node Split");
        currId = 11;
        t1.begin();
        long start = System.currentTimeMillis();
        long acc = 0;
        for(int i = 0; i < 18*11; ++i){
            long start1 = System.currentTimeMillis();
            bPlusIndex.insert(currId++, new EmployeeRecord(currId - 1, "Kostas", "Ferles"));
            acc += (System.currentTimeMillis() - start1);
        }
        t1.commit();
        t1.end();
        System.out.println("Time: " + (System.currentTimeMillis() - start));
        System.out.println("Mean: " + (double)acc/(18*11));
        t1.begin();
        System.out.println(bPlusIndex.toString());
        t1.commit();
        t1.end();

        System.out.println("Testing Search");
        for(int i = 0; i < 209; ++i){
            t1.begin();
            EmployeeRecord rec = bPlusIndex.get(i);
            assert rec.getId() == i;
            t1.commit();
            t1.end();
        }
        t1.begin();
        EmployeeRecord rec = bPlusIndex.get(209);
        assert rec == null;
        t1.commit();
        t1.end();

        currId = 0;
        Files.delete(p);
        bPlusIndex = new BPlusIndex<>(fileName, true, employeeFactory, employeeKeyValFactory);
        System.out.println("Testing Shifting Keys in Inner nodes.");
        System.out.println("Initial phase");
        List<Integer> insertIds = new ArrayList<>();
        for(int i = 0; i < 12; ++i, ++currId){
            t1.begin();
            int id;
            if(i < 6)
                id = currId;
            else
                id = currId*30;
            insertIds.add(id);
            bPlusIndex.insert(id, new EmployeeRecord(id, "Kostas", "Ferles"));
            t1.commit();
            t1.end();
        }

        t1.begin();
        System.out.println(bPlusIndex.toString());
        t1.commit();
        t1.end();


        Set<Integer> roundNumGen = new HashSet<>();
        Random r = new Random();
        for(int j = 0; j < 16; ++j){
            currId = 6;
            int salt = r.nextInt(16);
            while(roundNumGen.contains(salt)){
                salt = r.nextInt(16);
            }
            roundNumGen.add(salt);
            System.out.println("Phase: " + j + "("+ salt +")");
            for(int i = 0; i < 6; ++i, ++currId){
                t1.begin();
                int id = currId + 11*salt;
                insertIds.add(id);
                bPlusIndex.insert(id, new EmployeeRecord(id, "Kostas", "Ferles"));
                t1.commit();
                t1.end();
            }

            t1.begin();
            System.out.println(bPlusIndex.toString());
            t1.commit();
            t1.end();
        }

        System.out.println("Testing searching once again...");
        t1.begin();
        for(Integer id : insertIds)
            assert bPlusIndex.get(id).getId() == id;
        assert bPlusIndex.get(-1) == null;
        assert bPlusIndex.get(350) == null;
        assert bPlusIndex.get(301) == null;
        t1.commit();
        t1.end();

        Files.delete(p);
        bPlusIndex = new BPlusIndex<>(fileName, true, employeeFactory, employeeKeyValFactory);
        Set<Integer> newIds = new HashSet<>();
        System.out.println("Stress test...");

        for(int i = 0; i < 300; ++i){
            t1.begin();
            System.out.println(i);
            int id = r.nextInt();
            while(bPlusIndex.get(id) != null)
                id = r.nextInt();
            assert !newIds.contains(id);
            newIds.add(id);
            bPlusIndex.insert(id, new EmployeeRecord(id, "Kostas", "Ferles"));
            t1.commit();
            t1.end();
        }
        t1.begin();
        System.out.println(bPlusIndex.toString() + newIds.size());
        t1.commit();
        t1.end();
    }

}
