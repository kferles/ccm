package test;

import exception.InvalidBlockExcepxtion;
import file.blockfile.Block;
import file.blockfile.BlockFile;
import xaction.Xaction;

import java.io.IOException;

/**
 * Created by kostas on 4/16/14.
 */
public class TestBlockFile {

    private static String filename = "test_block_file";

    private static BlockFile testCreateAndOpen() throws IOException {
        return new BlockFile(filename);
    }

    private static Block allocateNewBlock(BlockFile bf) throws IOException {
        return bf.allocateNewBlock();
    }

    public static void main(String[] args) throws InvalidBlockExcepxtion{

        try {
            Xaction t1 = new Xaction();
            BlockFile bf = testCreateAndOpen();
            t1.begin();
            System.out.println(bf.toString());
            allocateNewBlock(bf);
            allocateNewBlock(bf);
            allocateNewBlock(bf);
            t1.commit();
            t1.end();

            t1.begin();
            System.out.println(bf.toString());
            Block b = allocateNewBlock(bf);
            bf.disposeBlock(b);
            t1.commit();
            t1.end();

            t1.begin();
            System.out.println(bf.toString());
            b = bf.loadBlock(2);
            bf.disposeBlock(b);
            t1.commit();
            t1.end();

            t1.begin();
            System.out.println(bf.toString());
            b = bf.loadBlock(3);
            bf.disposeBlock(b);
            t1.commit();
            t1.end();

            t1.begin();
            System.out.println(bf.toString());
            allocateNewBlock(bf);
            t1.commit();
            t1.end();

            t1.begin();
            System.out.println(bf.toString());
            t1.commit();
            t1.end();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
