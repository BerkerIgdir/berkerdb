package org.berkerdb.db.file;


import java.io.File;
import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class FileManager {

    private static final AtomicLong blockWriteCount = new AtomicLong();

    private final Path dbPath;

    private final Map<String, FileChannel> fileNameFileChannelMap = new HashMap<>();


    public FileManager(final String directoryName) {
        final String homeDir = System.getProperty("user.home");
        final Path dir = Paths.get(homeDir.concat(File.separator).concat(directoryName));
        final boolean isExist = Files.exists(dir);

        if (isExist) {
            try (final var dirStream = Files.newDirectoryStream(dir)) {
                for (var path : dirStream) {
                    if (path.startsWith("temp")) {
                        Files.delete(path);
                    }
                }
                dbPath = dir;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            try {
                dbPath = Files.createDirectories(dir);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public synchronized void read(final ByteBuffer byteBuffer, final Block block) {
        try {
            final var channel = getFileChannel(block.fileName());
            byteBuffer.clear();
            channel.read(byteBuffer, (long) block.blockNumber() * Page.BLOCK_SIZE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public synchronized void write(final ByteBuffer byteBuffer, final Block block) {
        try {
            final var channel = getFileChannel(block.fileName());
            byteBuffer.rewind();
            channel.write(byteBuffer, (long) block.blockNumber() * Page.BLOCK_SIZE);
            blockWriteCount.incrementAndGet();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void append(final ByteBuffer byteBuffer, final String fileName) {
        try {
            final var channel = getFileChannel(fileName);
            final var fileSize = channel.size();
            final var lastBlockNum = fileSize / Page.BLOCK_SIZE;
            byteBuffer.rewind();
            channel.write(byteBuffer, lastBlockNum * Page.BLOCK_SIZE);
            blockWriteCount.incrementAndGet();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private FileChannel getFileChannel(final String fileName) throws IOException {
        FileChannel fileChannel = fileNameFileChannelMap.get(fileName);

        if (Objects.nonNull(fileChannel)) {
            return fileChannel;
        }

        Path filePath = dbPath.resolve(fileName);
        if (!Files.exists(filePath)) {
            filePath = Files.createFile(filePath);
        }
        fileChannel = FileChannel.open(filePath, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.SYNC);
        fileNameFileChannelMap.put(fileName, fileChannel);
        return fileChannel;
    }

    public long getBlockWriteCount() {
        return blockWriteCount.get();
    }

    public long getFileSize(final String fileName) {
        try {
            return getFileChannel(fileName).size();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}