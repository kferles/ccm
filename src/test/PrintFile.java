package test;

import exception.InvalidBlockException;
import exception.InvalidKeyFactoryException;
import exception.InvalidRecordSize;
import file.index.BPlusIndex;
import file.record.sample.EmployeeFactory;
import file.record.sample.EmployeeKeyValFactory;
import file.record.sample.EmployeeRecord;
import xaction.Xaction;

import java.io.IOException;

public class PrintFile {

    public static void main(String[] args) throws InvalidRecordSize, IOException, InvalidKeyFactoryException, InvalidBlockException {
        EmployeeFactory recFac = new EmployeeFactory();
        EmployeeKeyValFactory keyFac = new EmployeeKeyValFactory();
        BPlusIndex<Integer, EmployeeRecord> index = new BPlusIndex<>(args[0], false, recFac, keyFac);
        Xaction t1 = new Xaction();
        t1.begin();
        System.out.println(index);
        t1.commit();
        t1.end();
    }
}
