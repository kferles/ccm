package util;

import config.ConfigParameters;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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

    public static void writeBufferToFile(FileChannel channel, ByteBuffer buf, int bufNum) throws IOException {
        assert bufNum > 0;

        buf.limit(bufferSize);
        buf.position(0);
        channel.position(bufferSize*bufNum);
        while (buf.hasRemaining())
            channel.write(buf);
        channel.force(true);
    }

}
