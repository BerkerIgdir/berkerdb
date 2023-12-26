package org.berkerdb.db.file;


import org.junit.jupiter.api.Test;

import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static org.berkerdb.db.file.Page.BLOCK_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FileMgrTest {

    @Test
    public void fileOperationsTest() throws IOException {
        final String homeDir = System.getProperty("user.home");
        final Path tempPath = Paths.get(homeDir);
        final Path tempDir = Files.createTempDirectory(tempPath, "temp");
        final Path tempFile = Files.createTempFile(tempDir, "test", ".db");
        final String testString = "test";
        final var byteBuffer = ByteBuffer.allocateDirect(2048);
        byteBuffer.put(testString.getBytes(StandardCharsets.UTF_8));

        byteBuffer.rewind();

        final var fileChan = Files.newByteChannel(tempFile, StandardOpenOption.WRITE);
        fileChan.write(byteBuffer);
        fileChan.close();

        byteBuffer.clear();

        final var readChan = Files.newByteChannel(tempFile, StandardOpenOption.READ);
        readChan.close();

        final var byteArray = new byte[4];

        byteBuffer.get(byteArray);

        assertEquals(0, testString.compareToIgnoreCase(new String(byteArray, StandardCharsets.UTF_8)));

        Files.deleteIfExists(tempFile);
        Files.deleteIfExists(tempDir);
    }

    @Test
    public void fileManagerReadAndWriteTest() throws IOException {

        final Block block = new Block("test_file", 12);
        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(BLOCK_SIZE);
        byteBuffer.put("test".getBytes(StandardCharsets.UTF_8));

        final FileManager fileManager = new FileManager("test");

        fileManager.write(byteBuffer, block);

        fileManager.read(byteBuffer, block);
        final var byteArray = new byte[4];
        byteBuffer.clear();
        byteBuffer.get(byteArray);


        //Delete the temp files.
        final String homeDirectory = System.getProperty("user.home");
        final Path homePath = Paths.get(homeDirectory);
        final Path dirPath = homePath.resolve("test");
        Files.deleteIfExists(dirPath.resolve("test_file"));
        Files.deleteIfExists(dirPath);

        assertEquals("test", new String(byteArray));
    }

}
