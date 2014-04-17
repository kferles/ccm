package test;

import exception.InvalidBlockExcepxtion;
import file.Block;
import file.BlockFile;

import java.io.IOException;

/**
 * Created by kostas on 4/16/14.
 */
public class TestBlockFile {

    private static String filename = "test_block_file";

    private static BlockFile testCreateAndOpen() throws IOException {
        return new BlockFile(filename);
    }

    public static void main(String[] args) throws InvalidBlockExcepxtion{

        try {
            BlockFile bf = testCreateAndOpen();
            System.out.println(bf.toString());
            bf = testCreateAndOpen();
            System.out.println(bf.toString());
            Block firstBlock = bf.allocateNewBlock();
            bf.commitBlock(firstBlock);
            System.out.println(bf.toString());
            bf.disposeBlock(bf.loadBlock(1));
            System.out.println(bf.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
