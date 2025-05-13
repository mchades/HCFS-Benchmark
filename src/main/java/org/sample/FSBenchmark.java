/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1)
@Warmup(iterations = 0)
@Measurement(iterations = 1)
public class FSBenchmark {

    @Param({"DEFAULT"})
    private String fsConfPath;
    
    @Param({"/tmp"})
    private String testBaseDirPath; // Base directory for all test files and directories

    private FileSystem fs;
    private Path testDir;
    private Path testFile;
    private Path createTestDir; // Dedicated directory for file creation tests
    private Path listTestDir; // Dedicated directory for list directory tests
    private Path deleteTestFile; // File for delete operation tests
    private Path appendTestFile; // File for append operation tests
    private Path renameSourceFile; // File for rename operation tests
    private Path renameTargetFile; // Target file for rename operation tests
    private Path mkdirsTestDir; // Directory for mkdirs operation tests
    private byte[] data;
    private boolean supportsAppend = false; // Flag to track if the filesystem supports append
    private final int dataSize = 1024 * 1024; // 1MB
    private final Random random = new Random();

    public static void main(String[] args) throws RunnerException {
        // Configure and run the benchmark tests
        Options options = new OptionsBuilder()
            .include(FSBenchmark.class.getSimpleName())
            .forks(1)
            .build();
        new Runner(options).run();
    }

    @Setup
    public void setup() throws IOException {
        initializeFileSystem();
        prepareTestData();
        createTestDirectories();
        preCreateListTestFiles(100);
        preCreateDeleteTestFile();
        checkAppendSupport(); // Check if FS supports append
        if (supportsAppend) {
            preCreateAppendTestFile(); // Only create append test file if append is supported
        }
        preCreateRenameTestFiles();
        prepareMkdirsTestDir();
    }

    private void checkAppendSupport() {
        // Check if filesystem supports append operation
        try {
            Path testAppendPath = new Path(testDir, "append-support-test");
            FSDataOutputStream out = fs.create(testAppendPath);
            out.close();

            try {
                out = fs.append(testAppendPath);
                out.close();
                supportsAppend = true; // If we get here, append is supported
                System.out.println("FileSystem supports append operation.");
            } catch (UnsupportedOperationException e) {
                System.out.println("FileSystem does not support append operation, will skip append benchmark.");
            } catch (Exception e) {
                System.out.println("Error testing append support: " + e.getMessage());
            } finally {
                fs.delete(testAppendPath, false);
            }
        } catch (Exception e) {
            System.out.println("Error checking append support: " + e.getMessage());
            supportsAppend = false;
        }
    }

    @TearDown(Level.Iteration)
    public void iterationTearDown() throws IOException {
        // Clean up after each iteration
        if (fs != null && createTestDir != null) {
            fs.delete(createTestDir, true);
            fs.mkdirs(createTestDir);
        }

        if (fs != null && listTestDir != null) {
            fs.delete(listTestDir, true);
            fs.mkdirs(listTestDir);

            // Recreate files for list directory tests
            for (int i = 0; i < 10; i++) {
                Path file = new Path(listTestDir, "list-file-" + i);
                FSDataOutputStream out = fs.create(file);
                out.close();
            }
        }

        if (fs != null && deleteTestFile != null) {
            if (fs.exists(deleteTestFile)) {
                fs.delete(deleteTestFile, false);
            }
            FSDataOutputStream out = fs.create(deleteTestFile);
            out.write(data);
            out.close();
        }

        if (fs != null && appendTestFile != null && supportsAppend) {
            if (fs.exists(appendTestFile)) {
                fs.delete(appendTestFile, false);
            }
            preCreateAppendTestFile(); // Recreate the file for append tests
        }

        if (fs != null && renameSourceFile != null && renameTargetFile != null) {
            if (fs.exists(renameTargetFile)) {
                fs.delete(renameTargetFile, false);
            }
            if (!fs.exists(renameSourceFile)) {
                FSDataOutputStream out = fs.create(renameSourceFile);
                out.write(data);
                out.close();
            }
        }

        if (fs != null && mkdirsTestDir != null) {
            fs.delete(mkdirsTestDir, true);
            fs.mkdirs(mkdirsTestDir);
        }
    }

    @TearDown
    public void tearDown() throws IOException {
        // Clean up the test directory
        if (fs != null && testDir != null) {
            fs.delete(testDir, true);
        }

        if (fs != null) {
            fs.close();
        }
    }

    @Benchmark
    public void testFileCreation() throws IOException {
        // Create a file in the dedicated test-create directory
        Path randomFile = new Path(createTestDir, "file-" + random.nextInt(1000000));
      try (FSDataOutputStream out = fs.create(randomFile)) {
          // do nothing, just create the file
      }
    }

    @Benchmark
    public void testFileAppend() throws IOException {
        // Skip the benchmark if append is not supported
        if (!supportsAppend) {
            return; // Skip this benchmark
        }
        
        // Measure only the append operation
        FSDataOutputStream out = fs.append(appendTestFile);
        out.write(data, 0, 1024 * 8); // Append 8KB of data
        out.close();
    }

    @Benchmark
    public void testFileDelete() throws IOException {
        // Measure only the delete operation
        fs.delete(deleteTestFile, false);
    }

    @Benchmark
    public void testListDirectory() throws IOException {
        // Measure only the listStatus operation
        fs.listStatus(listTestDir);
    }

    @TearDown(Level.Invocation)
    public void afterTestFileDelete() throws IOException {
        // Recreate the file after each delete operation
        FSDataOutputStream out = fs.create(deleteTestFile);
        out.write(data);
        out.close();
    }

    @Benchmark
    public boolean testRename() throws IOException {
        // Measure only the rename operation
        return fs.rename(renameSourceFile, renameTargetFile);
    }

    @TearDown(Level.Invocation)
    public void afterTestRename() throws IOException {
        // Recreate the source file for the next iteration
        FSDataOutputStream out = fs.create(renameSourceFile);
        out.write(data);
        out.close();
    }

    @Benchmark
    public void testGetFileStatus() throws IOException {
        // Measure only the getFileStatus operation
        fs.getFileStatus(testFile);
    }

    @Benchmark
    public FileStatus[] testListStatus() throws IOException {
        // Measure only the listStatus operation
        return fs.listStatus(listTestDir);
    }

    @Benchmark
    public boolean testMkdirs() throws IOException {
        // Measure only the mkdirs operation
        Path randomDir = new Path(mkdirsTestDir, "dir-" + random.nextInt(1000000));
        return fs.mkdirs(randomDir);
    }

    private void initializeFileSystem() throws IOException {
        // Initialize Hadoop configuration
        Configuration conf = new Configuration();
        if (fsConfPath != null && !fsConfPath.equals("DEFAULT")) {
            Path confPath = new Path(fsConfPath);
            
            // Verify the configuration file exists and is accessible
            try {
                // Create a temporary filesystem to check if the configuration file exists
                FileSystem localFs = FileSystem.getLocal(new Configuration());
                if (!localFs.exists(confPath)) {
                    throw new IOException("Configuration file not found: " + fsConfPath);
                }
                
                // Try to add the resource
                conf.addResource(confPath);
                System.out.println("Loaded configuration from: " + fsConfPath);
            } catch (Exception e) {
                System.err.println("ERROR: Failed to load configuration from " + fsConfPath + ": " + e.getMessage());
                throw new IOException("Failed to load configuration file: " + fsConfPath, e);
            }
        } else {
            // Use the local file system for default
            conf.set("fs.defaultFS", "file:///");
            System.out.println("Using local filesystem (default)");
        }
        
        try {
            fs = FileSystem.get(conf);
            System.out.println("Successfully initialized FileSystem: " + fs.getUri());
        } catch (IOException e) {
            System.err.println("ERROR: Failed to initialize filesystem: " + e.getMessage());
            throw e;
        }
    }

    private void createTestDirectories() throws IOException {
        // Create main test directory using the specified base directory
        testDir = new Path(testBaseDirPath + "/hadoop-fs-benchmark-" + System.currentTimeMillis());
        fs.mkdirs(testDir);

        // Create subdirectories for specific tests
        createTestDir = new Path(testDir, "test-create");
        fs.mkdirs(createTestDir);

        listTestDir = new Path(testDir, "test-list");
        fs.mkdirs(listTestDir);

        // Create and initialize testFile with data
        testFile = new Path(testDir, "test-file-status");
        FSDataOutputStream out = fs.create(testFile);
        out.write(data); // Write initial data to the test file
        out.close();
    }

    private void prepareTestData() {
        // Prepare test data
        data = new byte[dataSize];
        random.nextBytes(data);
    }

    private void preCreateListTestFiles(int fileCount) throws IOException {
        // Pre-create files for list directory tests
        for (int i = 0; i < fileCount; i++) {
            Path file = new Path(listTestDir, "list-file-" + i);
            FSDataOutputStream out = fs.create(file);
            out.close();
        }
    }

    private void preCreateDeleteTestFile() throws IOException {
        // Pre-create a file for delete operation tests
        deleteTestFile = new Path(testDir, "delete-file");
        FSDataOutputStream out = fs.create(deleteTestFile);
        out.write(data);
        out.close();
    }

    private void preCreateAppendTestFile() throws IOException {
        // Only create the file if append is supported
        if (!supportsAppend) {
            return;
        }
        
        // Pre-create a file for append operation tests
        appendTestFile = new Path(testDir, "append-file");
        FSDataOutputStream out = fs.create(appendTestFile);
        out.write(data); // Write initial data to the file
        out.close();
    }

    private void preCreateRenameTestFiles() throws IOException {
        // Pre-create source and target files for rename operation tests
        renameSourceFile = new Path(testDir, "rename-source-file");
        renameTargetFile = new Path(testDir, "rename-target-file");
        FSDataOutputStream out = fs.create(renameSourceFile);
        out.write(data);
        out.close();
    }

    private void prepareMkdirsTestDir() {
        // Prepare a directory path for mkdirs tests
        mkdirsTestDir = new Path(testDir, "mkdirs-test-dir");
    }
}
