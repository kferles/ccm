package util;

import config.ConfigParameters;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Created by kostas on 4/15/14.
 */
public class FileSystemMethods {

    private static FileSystem fs = FileSystems.getDefault();

    private static int bufferSize = ConfigParameters.getInstance().getBufferSize();

    public static Path getPath(String filename){
        return fs.getPath(filename);
    }

    public static boolean exists(Path p){
        return Files.exists(p);
    }

    public static void createFile(Path p) throws IOException {
        Files.createFile(p);
    }

}
