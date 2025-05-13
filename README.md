# HCFS-Benchmark

A JMH (Java Microbenchmark Harness) based Hadoop Compatible File System (HCFS) performance testing tool designed to evaluate various filesystem operations.

## Features

* Benchmarks for multiple filesystem operations
* Configurable test parameters
* Support for different filesystem implementations via configuration files
* Automatic detection of append operation support

## Benchmarked Operations

* File creation (create)
* File write (write)
* File append (append) - automatically skipped for unsupported filesystems
* File deletion (delete)
* File renaming (rename)
* Directory creation (mkdirs)
* File status retrieval (getFileStatus)
* Directory listing (listStatus)

## Prerequisites

* Java 1.8 or higher
* Maven 3.0 or higher
* Hadoop environment (optional, depending on the filesystem to be tested)

## Building the Project

```bash
cd /path/to/fs-test
mvn clean package
```

After successful build, the `benchmarks.jar` file will be generated in the `target` directory.

## Usage

### Basic Usage

```bash
java -jar target/benchmarks.jar
```

By default, the tests will use the local filesystem.

### Specifying a Configuration File

To test a specific filesystem (like HDFS), use the `fsConfPath` parameter:

```bash
java -jar target/benchmarks.jar -p fsConfPath=/path/to/core-site.xml
```

### Running Specific Benchmarks

To run specific benchmark methods, use the `-b` parameter:

```bash
java -jar target/benchmarks.jar -b ".*testFileCreation.*"
```

### Common JMH Parameters

```bash
# Set warm-up and measurement iterations
java -jar target/benchmarks.jar -wi 3 -i 5

# Output results in JSON format
java -jar target/benchmarks.jar -rf json

# Specify number of threads
java -jar target/benchmarks.jar -t 4
```

## Interpreting Results

After completion, JMH will output the average execution time (in milliseconds by default) for each benchmark. These metrics can be used to compare the performance of different filesystem operations or to benchmark different filesystem implementations against each other.

## Notes

1. Some filesystems may not support all operations. For instance, certain filesystems don't support append operations, and the benchmark will automatically skip these tests.
2. For distributed filesystems like HDFS, ensure that the Hadoop environment is properly configured.
3. Benchmark results may be affected by system load, disk I/O, network conditions, etc.
4. The benchmark creates temporary directories in `/tmp`. Ensure you have sufficient permissions.

## Example Output

```
Benchmark                      (fsConfPath)  Mode  Cnt   Score   Error  Units
FSBenchmark.testFileAppend          DEFAULT  avgt       ≈ 10⁻⁴          ms/op
FSBenchmark.testFileCreation        DEFAULT  avgt        4.206          ms/op
FSBenchmark.testFileDelete          DEFAULT  avgt        0.224          ms/op
FSBenchmark.testGetFileStatus       DEFAULT  avgt        0.050          ms/op
FSBenchmark.testListDirectory       DEFAULT  avgt        5.483          ms/op
FSBenchmark.testListStatus          DEFAULT  avgt        4.586          ms/op
FSBenchmark.testMkdirs              DEFAULT  avgt        2.021          ms/op
FSBenchmark.testRename              DEFAULT  avgt        0.302          ms/op
```