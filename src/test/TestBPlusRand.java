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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class TestBPlusRand {

    public static void main(String[] args) throws IOException, InvalidKeyFactoryException, InvalidRecordSize, InvalidBlockExcepxtion, InvalidRecordException {
        ConfigParameters.getInstance().setMaxBuffNumber(500);
        //Making buffer size very small, only one record fits in a buffer.
        ConfigParameters.getInstance().setBufferSize(150);
        String fileName = "test_file_rand";
        Path p = FileSystemMethods.getPath(fileName);
        Files.deleteIfExists(p);

        EmployeeFactory employeeFactory = new EmployeeFactory();
        EmployeeKeyValFactory employeeKeyValFactory = new EmployeeKeyValFactory();
        BPlusIndex<Integer, EmployeeRecord> bPlusIndex = new BPlusIndex<>(fileName, true, employeeFactory, employeeKeyValFactory);

        Xaction t1 = new Xaction();
        Random r = new Random();
        Set<Integer> genIds = new HashSet<>();
        ArrayList<Integer> genIdsOrdered = new ArrayList<>();


        try{

            System.out.println("Inserting phase...");
            for(int i = 0;  i < 30000; ++i){
                Integer id = r.nextInt();
                while(genIds.contains(id))
                    id = r.nextInt();
                genIds.add(id);
                genIdsOrdered.add(id);

                t1.begin();
                bPlusIndex.insert(id, new EmployeeRecord(id, "Kostas" + id, "Ferles" + id));
                t1.commit();
                t1.end();
            }


            System.out.println("Deleting phase...\n");
            for(int i = 0; i < 30000; ++i){
                int nextIdx = r.nextInt(genIdsOrdered.size());
                Integer removeId = genIdsOrdered.remove(nextIdx);
                t1.begin();
                bPlusIndex.delete(removeId);
                t1.commit();
                t1.end();

                if(i >= 1000 && i%1000 == 0){
                    for(Integer id1 : genIdsOrdered){
                        t1.begin();
                        EmployeeRecord rec = bPlusIndex.get(id1);
                        assert rec.getId().equals(id1);
                        assert rec.getFirstName().compareTo("Kostas" + id1) == 0;
                        assert rec.getLastName().compareTo("Ferles" + id1) == 0;
                        t1.commit();
                        t1.end();
                    }
                }
            }

            t1.begin();
            System.out.println(bPlusIndex.toString());
            t1.commit();
            t1.end();

            System.out.println("Inserting phase 2...");
            for(int i = 0;  i < 30000; ++i){
                Integer id = r.nextInt();
                while(genIds.contains(id))
                    id = r.nextInt();
                genIds.add(id);
                genIdsOrdered.add(id);

                t1.begin();
                bPlusIndex.insert(id, new EmployeeRecord(id, "Kostas" + id, "Ferles" + id));
                t1.commit();
                t1.end();
            }

            System.out.println("Deleting phase 2...\n");
            for(int i = 0; i < 30000; ++i){
                int nextIdx = r.nextInt(genIdsOrdered.size());
                Integer removeId = genIdsOrdered.remove(nextIdx);
                t1.begin();
                bPlusIndex.delete(removeId);
                t1.commit();
                t1.end();

                if(i >= 1000 && i%1000 == 0){
                    for(Integer id1 : genIdsOrdered){
                        t1.begin();
                        EmployeeRecord rec = bPlusIndex.get(id1);
                        assert rec.getId().equals(id1);
                        assert rec.getFirstName().compareTo("Kostas" + id1) == 0;
                        assert rec.getLastName().compareTo("Ferles" + id1) == 0;
                        t1.commit();
                        t1.end();
                    }
                }
            }

            t1.begin();
            System.out.println(bPlusIndex.toString());
            t1.commit();
            t1.end();

        }catch (AssertionError err){
            err.printStackTrace();
            System.exit(1);
        }
    }

}
