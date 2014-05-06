package test;

import buffermanager.BufferManager;
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
import java.util.*;

public class TestBPlusIndex {

    public static void main(String[] args) throws IOException, InvalidRecordSize, InvalidBlockExcepxtion, InvalidKeyFactoryException, InvalidRecordException {

        //Temporary solution until I'll find the 'victimize pool' bug
        ConfigParameters.getInstance().setMaxBuffNumber(400);
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

        System.out.println("Testing leaf split with various insert positions...");
        for(int j = 0, newInsPos = 0; j < 12; ++j, newInsPos += 2){
            Files.delete(p);
            currId = 1;
            BufferManager.getInstance().reset();
            bPlusIndex = new BPlusIndex<>(fileName, true, employeeFactory, employeeKeyValFactory);
            t1.begin();
            for(int i = 0; i < 11; ++i, currId += 2){
                bPlusIndex.insert(currId, new EmployeeRecord(currId, "Kostas", "Ferles"));
            }
            t1.commit();
            t1.end();

            t1.begin();
            bPlusIndex.insert(newInsPos, new EmployeeRecord(newInsPos, "Kostas", "Ferles"));
            t1.commit();
            t1.end();

            t1.begin();
            System.out.println(bPlusIndex.toString());
            t1.commit();
            t1.end();
        }

        Files.delete(p);
        currId = 1;
        BufferManager.getInstance().reset();
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
        BufferManager.getInstance().reset();
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
        BufferManager.getInstance().reset();
        bPlusIndex = new BPlusIndex<>(fileName, true, employeeFactory, employeeKeyValFactory);
        Set<Integer> newIds = new HashSet<>();
        System.out.println("Stress test...");

        for(int i = 0; i < 300; ++i){
            t1.begin();
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

        //Testing delete
        System.out.println("Testing delete...");
        Files.delete(p);
        BufferManager.getInstance().reset();
        bPlusIndex = new BPlusIndex<>(fileName, true, employeeFactory, employeeKeyValFactory);

        //Deleting last element
        t1.begin();
        bPlusIndex.insert(0, new EmployeeRecord(0, "Kostas", "Ferles"));
        System.out.println(bPlusIndex.toString());

        bPlusIndex.delete(0);
        System.out.println(bPlusIndex.toString());
        t1.commit();
        t1.end();

        //Testing leaf shifting from neighbor
        System.out.println("Testing shifting after delete at leaf nodes");
        Files.delete(p);
        currId = 0;
        BufferManager.getInstance().reset();
        bPlusIndex = new BPlusIndex<>(fileName, true, employeeFactory, employeeKeyValFactory);
        t1.begin();
        for(int i = 0; i < 17; ++i, currId += 6){
            bPlusIndex.insert(currId, new EmployeeRecord(currId, "Kostas", "Ferles"));
        }
        System.out.println(bPlusIndex.toString());

        System.out.println("Borrowing from right leaf node");
        bPlusIndex.delete(0);
        System.out.println(bPlusIndex.toString());

        System.out.println("Borrowing from left leaf node");
        bPlusIndex.delete(54);
        bPlusIndex.delete(72);
        bPlusIndex.delete(90);

        System.out.println(bPlusIndex.toString());

        //filling up again rightmost leaf node
        bPlusIndex.insert(54, new EmployeeRecord(54, "Kostas", "Ferles"));
        bPlusIndex.insert(72, new EmployeeRecord(72, "Kostas", "Ferles"));
        bPlusIndex.insert(90, new EmployeeRecord(90, "Kostas", "Ferles"));
        bPlusIndex.insert(102, new EmployeeRecord(102, "Kostas", "Ferles"));
        bPlusIndex.insert(108, new EmployeeRecord(108, "Kostas", "Ferles"));
        t1.commit();
        t1.end();

        t1.begin();
        currId = 114;
        for(int i = 0; i < 16; ++i){
            for(int j = 0; j < 11; ++j, currId += 6)
                bPlusIndex.insert(currId, new EmployeeRecord(currId, "Kostas", "Ferles"));
        }

        System.out.println("Testing shift from right neighbor but different parent node");
        bPlusIndex.insert(407, new EmployeeRecord(407, "Kostas", "Ferles"));
        System.out.println(bPlusIndex.toString());

        bPlusIndex.delete(366);
        System.out.println(bPlusIndex.toString());

        System.out.println("Testing shift from left neighbor but different parent node");
        bPlusIndex.insert(366, new EmployeeRecord(366, "Kostas", "Ferles"));
        bPlusIndex.delete(402);
        System.out.println(bPlusIndex.toString());

        System.out.println("And one more round of the same");
        bPlusIndex.insert(727, new EmployeeRecord(727, "Kostas", "Ferles"));
        bPlusIndex.delete(744);

        System.out.println(bPlusIndex.toString());
        bPlusIndex.insert(728, new EmployeeRecord(727, "Kostas", "Ferles"));
        bPlusIndex.delete(726);
        System.out.println(bPlusIndex.toString());

        System.out.println("Testing leaf and inner node merging");
        bPlusIndex.delete(84);
        System.out.println(bPlusIndex.toString());

        bPlusIndex.delete(90);
        bPlusIndex.delete(96);
        bPlusIndex.delete(102);
        bPlusIndex.delete(108);
        bPlusIndex.delete(114);
        System.out.println(bPlusIndex.toString());

        bPlusIndex.delete(60);
        bPlusIndex.delete(48);
        System.out.println(bPlusIndex.toString());

        bPlusIndex.delete(6);
        bPlusIndex.delete(18);
        bPlusIndex.delete(12);
        bPlusIndex.delete(54);
        bPlusIndex.delete(66);
        bPlusIndex.delete(72);
        bPlusIndex.delete(150);
        bPlusIndex.delete(132);
        bPlusIndex.delete(120);
        System.out.println(bPlusIndex.toString());
        t1.commit();
        t1.end();
    }

}
