/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs;

import java.io.FileNotFoundException;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientHandler;
import org.apache.hadoop.fs.azurebfs.utils.DirectoryStateHelper;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.ReflectionUtils;

import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.ConcurrentWriteOperationDetectedException;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.ITestAbfsClient;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.apache.hadoop.test.LambdaTestUtils;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_MKDIR_OVERWRITE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsFile;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;

/**
 * Test create operation.
 */
public class ITestAzureBlobFileSystemCreate extends
    AbstractAbfsIntegrationTest {
  private static final Path TEST_FILE_PATH = new Path("testfile");
  private static final String TEST_FOLDER_PATH = "testFolder";
  private static final String TEST_CHILD_FILE = "childFile";

  public ITestAzureBlobFileSystemCreate() throws Exception {
    super();
  }

  @Test
  public void testEnsureFileCreatedImmediately() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    FSDataOutputStream out = fs.create(TEST_FILE_PATH);
    try {
      assertIsFile(fs, TEST_FILE_PATH);
    } finally {
      out.close();
    }
    assertIsFile(fs, TEST_FILE_PATH);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCreateNonRecursive() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFolderPath = path(TEST_FOLDER_PATH);
    Path testFile = new Path(testFolderPath, TEST_CHILD_FILE);
    try {
      fs.createNonRecursive(testFile, true, 1024, (short) 1, 1024, null);
      fail("Should've thrown");
    } catch (FileNotFoundException expected) {
    }
    fs.registerListener(new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.MKDIR, false, 0));
    fs.mkdirs(testFolderPath);
    fs.registerListener(null);

    fs.createNonRecursive(testFile, true, 1024, (short) 1, 1024, null)
        .close();
    assertIsFile(fs, testFile);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCreateNonRecursive1() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFolderPath = path(TEST_FOLDER_PATH);
    Path testFile = new Path(testFolderPath, TEST_CHILD_FILE);
    try {
      fs.createNonRecursive(testFile, FsPermission.getDefault(), EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE), 1024, (short) 1, 1024, null);
      fail("Should've thrown");
    } catch (FileNotFoundException expected) {
    }
    fs.mkdirs(testFolderPath);
    fs.createNonRecursive(testFile, true, 1024, (short) 1, 1024, null)
        .close();
    assertIsFile(fs, testFile);

  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCreateNonRecursive2() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();

    Path testFolderPath = path(TEST_FOLDER_PATH);
    Path testFile = new Path(testFolderPath, TEST_CHILD_FILE);
    try {
      fs.createNonRecursive(testFile, FsPermission.getDefault(), false, 1024, (short) 1, 1024, null);
      fail("Should've thrown");
    } catch (FileNotFoundException e) {
    }
    fs.mkdirs(testFolderPath);
    fs.createNonRecursive(testFile, true, 1024, (short) 1, 1024, null)
        .close();
    assertIsFile(fs, testFile);
  }

  @Test
  public void testCreateOnRoot() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFile = path(AbfsHttpConstants.ROOT_PATH);
    AbfsRestOperationException ex = intercept(AbfsRestOperationException.class, () ->
        fs.create(testFile, true));
    if (ex.getStatusCode() != HTTP_CONFLICT) {
      // Request should fail with 409.
      throw ex;
    }

    ex = intercept(AbfsRestOperationException.class, () ->
        fs.createNonRecursive(testFile, FsPermission.getDefault(),
            false, 1024, (short) 1, 1024, null));
    if (ex.getStatusCode() != HTTP_CONFLICT) {
      // Request should fail with 409.
      throw ex;
    }
  }

  /**
   * Attempts to use to the ABFS stream after it is closed.
   */
  @Test
  public void testWriteAfterClose() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFolderPath = path(TEST_FOLDER_PATH);
    Path testPath = new Path(testFolderPath, TEST_CHILD_FILE);
    FSDataOutputStream out = fs.create(testPath);
    out.close();
    intercept(IOException.class, () -> out.write('a'));
    intercept(IOException.class, () -> out.write(new byte[]{'a'}));
    // hsync is not ignored on a closed stream
    // out.hsync();
    out.flush();
    out.close();
  }

  /**
   * Attempts to double close an ABFS output stream from within a
   * FilterOutputStream.
   * That class handles a double failure on close badly if the second
   * exception rethrows the first.
   */
  @Test
  public void testTryWithResources() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFolderPath = path(TEST_FOLDER_PATH);
    Path testPath = new Path(testFolderPath, TEST_CHILD_FILE);
    try (FSDataOutputStream out = fs.create(testPath)) {
      out.write('1');
      out.hsync();
      // this will cause the next write to failAll
      fs.delete(testPath, false);
      out.write('2');
      out.hsync();
      fail("Expected a failure");
    } catch (IOException fnfe) {
      //appendblob outputStream does not generate suppressed exception on close as it is
      //single threaded code
      if (!fs.getAbfsStore().isAppendBlobKey(fs.makeQualified(testPath).toString())) {
        // the exception raised in close() must be in the caught exception's
        // suppressed list
        Throwable[] suppressed = fnfe.getSuppressed();
        Assertions.assertThat(suppressed.length)
            .describedAs("suppressed count should be 1").isEqualTo(1);
        Throwable inner = suppressed[0];
        if (!(inner instanceof IOException)) {
          throw inner;
        }
        GenericTestUtils.assertExceptionContains(fnfe.getMessage(), inner);
      }
    }
  }

  /**
   * Attempts to write to the azure stream after it is closed will raise
   * an IOException.
   */
  @Test
  public void testFilterFSWriteAfterClose() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testPath = new Path(TEST_FOLDER_PATH, TEST_CHILD_FILE);
    FSDataOutputStream out = fs.create(testPath);
    intercept(IOException.class,
        () -> {
          try (FilterOutputStream fos = new FilterOutputStream(out)) {
            byte[] bytes = new byte[8*ONE_MB];
            fos.write(bytes);
            fos.write(bytes);
            fos.flush();
            out.hsync();
            fs.delete(testPath, false);
            // trigger the first failure
            throw intercept(IOException.class,
                () -> {
                  fos.write('b');
                  out.hsync();
                  return "hsync didn't raise an IOE";
                });
          }
        });
  }

  /**
   * Tests if the number of connections made for:
   * 1. create overwrite=false of a file that doesnt pre-exist
   * 2. create overwrite=false of a file that pre-exists
   * 3. create overwrite=true of a file that doesnt pre-exist
   * 4. create overwrite=true of a file that pre-exists
   * matches the expectation when run against both combinations of
   * fs.azure.enable.conditional.create.overwrite=true and
   * fs.azure.enable.conditional.create.overwrite=false
   * @throws Throwable
   */
  @Test
  public void testDefaultCreateOverwriteFileTest() throws Throwable {
    testCreateFileOverwrite(true);
    testCreateFileOverwrite(false);
  }

  public void testCreateFileOverwrite(boolean enableConditionalCreateOverwrite)
      throws Throwable {
    final AzureBlobFileSystem currentFs = getFileSystem();
    Configuration config = new Configuration(this.getRawConfiguration());
    config.set("fs.azure.enable.conditional.create.overwrite",
        Boolean.toString(enableConditionalCreateOverwrite));
    AzureBlobFileSystemStore store = currentFs.getAbfsStore();
    AbfsClient client = store.getClientHandler().getIngressClient();

    final AzureBlobFileSystem fs =
        (AzureBlobFileSystem) FileSystem.newInstance(currentFs.getUri(),
            config);

    long totalConnectionMadeBeforeTest = fs.getInstrumentationMap()
        .get(CONNECTIONS_MADE.getStatName());

    int createRequestCount = 0;
    final Path nonOverwriteFile = new Path("/NonOverwriteTest_FileName_"
        + UUID.randomUUID().toString());

    // Case 1: Not Overwrite - File does not pre-exist
    // create should be successful
    fs.create(nonOverwriteFile, false);

    // One request to server to create path should be issued
    // two calls added for -
    // 1. getFileStatus on DFS endpoint : 1
    //    getFileStatus on Blob endpoint: 2 (Additional List blob call)
    // 2. actual create call: 1
    createRequestCount += (client instanceof AbfsBlobClient && !getIsNamespaceEnabled(fs) ? 3: 1);

    assertAbfsStatistics(
        CONNECTIONS_MADE,
        totalConnectionMadeBeforeTest + createRequestCount,
        fs.getInstrumentationMap());

    // Case 2: Not Overwrite - File pre-exists
    fs.registerListener(new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.CREATE, false, 0));
    intercept(FileAlreadyExistsException.class,
        () -> fs.create(nonOverwriteFile, false));
    fs.registerListener(null);

    // One request to server to create path should be issued
    // Only single tryGetFileStatus should happen
    // 1. getFileStatus on DFS endpoint : 1
    //    getFileStatus on Blob endpoint: 1 (No Additional List blob call as file exists)

    createRequestCount += (client instanceof AbfsBlobClient && !getIsNamespaceEnabled(fs) ? 2: 1);

    assertAbfsStatistics(
        CONNECTIONS_MADE,
        totalConnectionMadeBeforeTest + createRequestCount,
        fs.getInstrumentationMap());

    final Path overwriteFilePath = new Path("/OverwriteTest_FileName_"
        + UUID.randomUUID().toString());

    // Case 3: Overwrite - File does not pre-exist
    // create should be successful
    fs.create(overwriteFilePath, true);

    /// One request to server to create path should be issued
    // two calls added for -
    // 1. getFileStatus on DFS endpoint : 1
    //    getFileStatus on Blob endpoint: 2 (Additional List blob call for non-existing path)
    // 2. actual create call: 1
    createRequestCount += (client instanceof AbfsBlobClient && !getIsNamespaceEnabled(fs) ? 3: 1);

    assertAbfsStatistics(
        CONNECTIONS_MADE,
        totalConnectionMadeBeforeTest + createRequestCount,
        fs.getInstrumentationMap());

    // Case 4: Overwrite - File pre-exists
    fs.registerListener(new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.CREATE, true, 0));
    fs.create(overwriteFilePath, true);
    fs.registerListener(null);

    createRequestCount += (client instanceof AbfsBlobClient && !getIsNamespaceEnabled(fs) ? 1: 0);

    // Second actual create call will hap
    if (enableConditionalCreateOverwrite) {
      // Three requests will be sent to server to create path,
      // 1. create without overwrite
      // 2. GetFileStatus to get eTag
      // 3. create with overwrite
      createRequestCount += (client instanceof AbfsBlobClient && !getIsNamespaceEnabled(fs) ? 4: 3);
    } else {
      createRequestCount++;
    }

    assertAbfsStatistics(
        CONNECTIONS_MADE,
        totalConnectionMadeBeforeTest + createRequestCount,
        fs.getInstrumentationMap());
  }

  /**
   * Test negative scenarios with Create overwrite=false as default
   * With create overwrite=true ending in 3 calls:
   * A. Create overwrite=false
   * B. GFS
   * C. Create overwrite=true
   *
   * Scn1: A fails with HTTP409, leading to B which fails with HTTP404,
   *        detect parallel access
   * Scn2: A fails with HTTP409, leading to B which fails with HTTP500,
   *        fail create with HTTP500
   * Scn3: A fails with HTTP409, leading to B and then C,
   *        which fails with HTTP412, detect parallel access
   * Scn4: A fails with HTTP409, leading to B and then C,
   *        which fails with HTTP500, fail create with HTTP500
   * Scn5: A fails with HTTP500, fail create with HTTP500
   */
  @Test
  public void testNegativeScenariosForCreateOverwriteDisabled()
      throws Throwable {

    final AzureBlobFileSystem currentFs = getFileSystem();
    Configuration config = new Configuration(this.getRawConfiguration());
    config.set("fs.azure.enable.conditional.create.overwrite",
        Boolean.toString(true));

    final AzureBlobFileSystem fs =
        (AzureBlobFileSystem) FileSystem.newInstance(currentFs.getUri(),
            config);

    // Get mock AbfsClient with current config
    AbfsClient
        mockClient
        = ITestAbfsClient.getMockAbfsClient(
        fs.getAbfsStore().getClient(),
        fs.getAbfsStore().getAbfsConfiguration());
    AbfsClientHandler clientHandler = Mockito.mock(AbfsClientHandler.class);
    when(clientHandler.getIngressClient()).thenReturn(mockClient);
    when(clientHandler.getClient(Mockito.any())).thenReturn(mockClient);

    AzureBlobFileSystemStore abfsStore = fs.getAbfsStore();

    ReflectionUtils.setFinalField(AzureBlobFileSystemStore.class, abfsStore, "clientHandler", clientHandler);
    ReflectionUtils.setFinalField(AzureBlobFileSystemStore.class, abfsStore, "client", mockClient);
    boolean isNamespaceEnabled = abfsStore
        .getIsNamespaceEnabled(getTestTracingContext(fs, false));

    AbfsRestOperation successOp = mock(
        AbfsRestOperation.class);
    AbfsHttpOperation http200Op = mock(
        AbfsHttpOperation.class);
    when(http200Op.getStatusCode()).thenReturn(HTTP_OK);
    when(successOp.getResult()).thenReturn(http200Op);

    AbfsRestOperationException conflictResponseEx
        = getMockAbfsRestOperationException(HTTP_CONFLICT);
    AbfsRestOperationException serverErrorResponseEx
        = getMockAbfsRestOperationException(HTTP_INTERNAL_ERROR);
    AbfsRestOperationException fileNotFoundResponseEx
        = getMockAbfsRestOperationException(HTTP_NOT_FOUND);
    AbfsRestOperationException preConditionResponseEx
        = getMockAbfsRestOperationException(HTTP_PRECON_FAILED);

    // mock for overwrite=false
    doThrow(conflictResponseEx) // Scn1: GFS fails with Http404
        .doThrow(conflictResponseEx) // Scn2: GFS fails with Http500
        .doThrow(
            conflictResponseEx) // Scn3: create overwrite=true fails with Http412
        .doThrow(
            conflictResponseEx) // Scn4: create overwrite=true fails with Http500
        .doThrow(
            serverErrorResponseEx) // Scn5: create overwrite=false fails with Http500
        .when(mockClient)
        .createPath(any(String.class), eq(true), eq(false),
            any(AzureBlobFileSystemStore.Permissions.class), any(boolean.class), eq(null), any(),
            any(TracingContext.class));

    doThrow(fileNotFoundResponseEx) // Scn1: GFS fails with Http404
        .doThrow(serverErrorResponseEx) // Scn2: GFS fails with Http500
        .doReturn(successOp) // Scn3: create overwrite=true fails with Http412
        .doReturn(successOp) // Scn4: create overwrite=true fails with Http500
        .when(mockClient)
        .getPathStatus(any(String.class), eq(false), any(TracingContext.class), nullable(
            ContextEncryptionAdapter.class));

    // mock for overwrite=true
    doThrow(
        preConditionResponseEx) // Scn3: create overwrite=true fails with Http412
        .doThrow(
            serverErrorResponseEx) // Scn4: create overwrite=true fails with Http500
        .when(mockClient)
        .createPath(any(String.class), eq(true), eq(true),
            any(AzureBlobFileSystemStore.Permissions.class), any(boolean.class), eq(null), any(),
            any(TracingContext.class));

    // Scn1: GFS fails with Http404
    // Sequence of events expected:
    // 1. create overwrite=false - fail with conflict
    // 2. GFS - fail with File Not found
    // Create will fail with ConcurrentWriteOperationDetectedException
    validateCreateFileException(ConcurrentWriteOperationDetectedException.class,
        abfsStore);

    // Scn2: GFS fails with Http500
    // Sequence of events expected:
    // 1. create overwrite=false - fail with conflict
    // 2. GFS - fail with Server error
    // Create will fail with 500
    validateCreateFileException(AbfsRestOperationException.class, abfsStore);

    // Scn3: create overwrite=true fails with Http412
    // Sequence of events expected:
    // 1. create overwrite=false - fail with conflict
    // 2. GFS - pass
    // 3. create overwrite=true - fail with Pre-Condition
    // Create will fail with ConcurrentWriteOperationDetectedException
    validateCreateFileException(ConcurrentWriteOperationDetectedException.class,
        abfsStore);

    // Scn4: create overwrite=true fails with Http500
    // Sequence of events expected:
    // 1. create overwrite=false - fail with conflict
    // 2. GFS - pass
    // 3. create overwrite=true - fail with Server error
    // Create will fail with 500
    validateCreateFileException(AbfsRestOperationException.class, abfsStore);

    // Scn5: create overwrite=false fails with Http500
    // Sequence of events expected:
    // 1. create overwrite=false - fail with server error
    // Create will fail with 500
    validateCreateFileException(AbfsRestOperationException.class, abfsStore);
  }

  private <E extends Throwable> void validateCreateFileException(final Class<E> exceptionClass, final AzureBlobFileSystemStore abfsStore)
      throws Exception {
    FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL,
        FsAction.ALL);
    FsPermission umask = new FsPermission(FsAction.NONE, FsAction.NONE,
        FsAction.NONE);
    Path testPath = new Path("/testFile");
    intercept(
        exceptionClass,
        () -> abfsStore.createFile(testPath, null, true, permission, umask,
            getTestTracingContext(getFileSystem(), true)));
  }

  private AbfsRestOperationException getMockAbfsRestOperationException(int status) {
    return new AbfsRestOperationException(status, "", "", new Exception());
  }


  /**
   * Attempts to test multiple flush calls.
   */
  @Test
  public void testMultipleFlush() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testPath = new Path(TEST_FOLDER_PATH, TEST_CHILD_FILE);
    try (FSDataOutputStream out = fs.create(testPath)) {
      out.write('1');
      out.hsync();
      out.write('2');
      out.hsync();
    }
  }

  /**
   * Delete the blob before flush and verify that an exception should be thrown.
   */
  @Test
  public void testDeleteBeforeFlush() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testPath = new Path(TEST_FOLDER_PATH, TEST_CHILD_FILE);
    try (FSDataOutputStream out = fs.create(testPath)) {
      out.write('1');
      fs.delete(testPath, false);
      out.hsync();
      // this will cause the next write to failAll
    } catch (IOException fnfe) {
      //appendblob outputStream does not generate suppressed exception on close as it is
      //single threaded code
      if (!fs.getAbfsStore().isAppendBlobKey(fs.makeQualified(testPath).toString())) {
        // the exception raised in close() must be in the caught exception's
        // suppressed list
        Throwable[] suppressed = fnfe.getSuppressed();
        assertEquals("suppressed count", 1, suppressed.length);
        Throwable inner = suppressed[0];
        if (!(inner instanceof IOException)) {
          throw inner;
        }
        GenericTestUtils.assertExceptionContains(fnfe.getMessage(), inner.getCause(), inner.getCause().getMessage());
      }
    }
  }

  /**
   * Creating subdirectory on existing file path should fail.
   * @throws Exception
   */
  @Test
  public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.create(new Path("a/b/c"));
    fs.mkdirs(new Path("a/b/d"));
    intercept(IOException.class, () -> fs.mkdirs(new Path("a/b/c/d/e")));

    Assertions.assertThat(fs.exists(new Path("a/b/c"))).isTrue();
    Assertions.assertThat(fs.exists(new Path("a/b/d"))).isTrue();
    // Asserting directory created still exists as explicit.
    Assertions.assertThat(
            DirectoryStateHelper.isExplicitDirectory(new Path("a/b/d"), fs,
                getTestTracingContext(fs, true)))
        .describedAs("Path is not an explicit directory")
        .isTrue();
  }

  /**
   * Try creating file same as an existing directory.
   * @throws Exception
   */
  @Test
  public void testCreateDirectoryAndFile() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a/b/c"));
    Assertions.assertThat(fs.exists(new Path("a/b/c"))).isTrue();
    intercept(IOException.class, () -> fs.create(new Path("a/b/c")));
    // Asserting that directory still exists as explicit
    Assertions.assertThat(
            DirectoryStateHelper.isExplicitDirectory(new Path("a/b/c"),
                fs, getTestTracingContext(fs, true)))
        .describedAs("Path is not an explicit directory")
        .isTrue();
  }

  /**
   * Creating same file without specifying overwrite.
   * @throws Exception
   */
  @Test
  public void testCreateSameFile() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.create(new Path("a/b/c"));
    fs.create(new Path("a/b/c"));
    Assertions.assertThat(fs.exists(new Path("a/b/c")))
        .describedAs("Path does not exist")
        .isTrue();
  }

  /**
   * Creating same file with overwrite flag set to false.
   * @throws Exception
   */
  @Test
  public void testCreateSameFileWithOverwriteFalse() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.create(new Path("a/b/c"));
    Assertions.assertThat(fs.exists(new Path("a/b/c")))
        .describedAs("Path does not exist")
        .isTrue();
    intercept(IOException.class, () -> fs.create(new Path("a/b/c"), false));
  }

  /**
   * Creation of already existing subpath should fail.
   * @throws Exception
   */
  @Test
  public void testCreateSubPath() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.create(new Path("a/b/c"));
    Assertions.assertThat(fs.exists(new Path("a/b/c")))
        .describedAs("Path does not exist")
        .isTrue();
    intercept(IOException.class, () -> fs.create(new Path("a/b")));
  }

  /**
   * Creating path with parent explicit.
   */
  @Test
  public void testCreatePathParentExplicit() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a/b/c"));
    Assertions.assertThat(fs.exists(new Path("a/b/c")))
        .describedAs("Path does not exist")
        .isTrue();
    fs.create(new Path("a/b/c/d"));
    Assertions.assertThat(fs.exists(new Path("a/b/c/d")))
        .describedAs("Path does not exist")
        .isTrue();

    // asserting that parent stays explicit
    Assertions.assertThat(
            DirectoryStateHelper.isExplicitDirectory(new Path("a/b/c"),
                fs, getTestTracingContext(fs, true)))
        .describedAs("Path is not an explicit directory")
        .isTrue();
  }

  /**
   * Test create on implicit directory with explicit parent.
   * @throws Exception
   */
  @Test
  public void testParentExplicitPathImplicit() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Assume.assumeTrue(getIngressServiceType() == AbfsServiceType.BLOB);
    fs.mkdirs(new Path("/explicitParent"));
    String sourcePathName = "/explicitParent/implicitDir";
    Path sourcePath = new Path(sourcePathName);
    createAzCopyFolder(sourcePath);

    intercept(IOException.class, () ->
        fs.create(sourcePath, true));
    intercept(IOException.class, () ->
        fs.create(sourcePath, false));

    Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(sourcePath.getParent(), fs, getTestTracingContext(fs, true)))
        .describedAs("Parent directory should be explicit.")
        .isTrue();
    Assertions.assertThat(DirectoryStateHelper.isImplicitDirectory(sourcePath, fs, getTestTracingContext(fs, true)))
        .describedAs("Path should be implicit.")
        .isTrue();
  }

  /**
   * Test create on implicit directory with implicit parent
   * @throws Exception
   */
  @Test
  public void testParentImplicitPathImplicit() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Assume.assumeTrue(getIngressServiceType() == AbfsServiceType.BLOB);
    String parentPathName = "/implicitParent";
    Path parentPath = new Path(parentPathName);
    String sourcePathName = "/implicitParent/implicitDir";
    Path sourcePath = new Path(sourcePathName);

    createAzCopyFolder(parentPath);
    createAzCopyFolder(sourcePath);

    intercept(IOException.class, () ->
        fs.create(sourcePath, true));
    intercept(IOException.class, () ->
        fs.create(sourcePath, false));

    Assertions.assertThat(DirectoryStateHelper.isImplicitDirectory(parentPath, fs, getTestTracingContext(fs, true)))
        .describedAs("Parent directory is implicit.")
        .isTrue();
    Assertions.assertThat(DirectoryStateHelper.isImplicitDirectory(sourcePath, fs, getTestTracingContext(fs, true)))
        .describedAs("Path should also be implicit.")
        .isTrue();
  }

  /**
   * Tests create file when file exists already
   * Verifies using eTag for overwrite = true/false
   */
  @Test
  public void testCreateFileExistsImplicitParent() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final AzureBlobFileSystemStore store = fs.getAbfsStore();
    String parentPathName = "/implicitParent";
    Path parentPath = new Path(parentPathName);
    createAzCopyFolder(parentPath);

    String fileName = "/implicitParent/testFile";
    Path filePath = new Path(fileName);
    fs.create(filePath);
    String eTag = extractFileEtag(fileName);

    // testing createFile on already existing file path
    fs.create(filePath, true);

    String eTagAfterCreateOverwrite = extractFileEtag(fileName);

    Assertions.assertThat(eTag.equals(eTagAfterCreateOverwrite))
        .describedAs("New file eTag after create overwrite should be different from old")
        .isFalse();

    intercept(IOException.class, () ->
        fs.create(filePath, false));

    String eTagAfterCreate = extractFileEtag(fileName);

    Assertions.assertThat(eTagAfterCreateOverwrite.equals(eTagAfterCreate))
        .describedAs("File eTag should not change as creation fails")
        .isTrue();

    Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(parentPath, fs, getTestTracingContext(fs, true)))
        .describedAs("Parent path should also change to explicit.")
        .isTrue();
  }

  /**
   * Tests create file when the parent is an existing file
   * should fail.
   * @throws Exception FileAlreadyExists for blob and IOException for dfs.
   */
  @Test
  public void testCreateFileParentFile() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final AzureBlobFileSystemStore store = fs.getAbfsStore();

    String parentName = "/testParentFile";
    Path parent = new Path(parentName);
    fs.create(parent);

    String childName = "/testParentFile/testChildFile";
    Path child = new Path(childName);
    IOException e = intercept(IOException.class, () ->
        fs.create(child, false));

    // asserting that parent stays explicit
    FileStatus status = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path(parentName)),
        new TracingContext(getTestTracingContext(fs, true)));
    Assertions.assertThat(status.isDirectory())
        .describedAs("Path is not a file")
        .isFalse();
  }

  /**
   * Creating directory on existing file path should fail.
   * @throws Exception
   */
  @Test
  public void testCreateMkdirs() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.create(new Path("a/b/c"));
    intercept(IOException.class, () -> fs.mkdirs(new Path("a/b/c/d")));
  }

  /**
   * Test mkdirs.
   * @throws Exception
   */
  @Test
  public void testMkdirs() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a/b"));
    fs.mkdirs(new Path("a/b/c/d"));
    fs.mkdirs(new Path("a/b/c/e"));

    Assertions.assertThat(fs.exists(new Path("a/b")))
        .describedAs("Path a/b does not exist")
        .isTrue();
    Assertions.assertThat(fs.exists(new Path("a/b/c/d")))
        .describedAs("Path a/b/c/d does not exist")
        .isTrue();
    Assertions.assertThat(fs.exists(new Path("a/b/c/e")))
        .describedAs("Path a/b/c/e does not exist")
        .isTrue();

    // Asserting that directories created as explicit
    FileStatus status = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path("a/b")),
        new TracingContext(getTestTracingContext(fs, true)));
    Assertions.assertThat(status.isDirectory())
        .describedAs("Path a/b is not an explicit directory")
        .isTrue();
    FileStatus status1 = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path("a/b/c/d")),
        new TracingContext(getTestTracingContext(fs, true)));
    Assertions.assertThat(status1.isDirectory())
        .describedAs("Path a/b/c/d is not an explicit directory")
        .isTrue();
    FileStatus status2 = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path("a/b/c/e")),
        new TracingContext(getTestTracingContext(fs, true)));
    Assertions.assertThat(status2.isDirectory())
        .describedAs("Path a/b/c/e is not an explicit directory")
        .isTrue();
  }

  /**
   * Creating subpath of directory path should fail.
   * @throws Exception
   */
  @Test
  public void testMkdirsCreateSubPath() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a/b/c"));
    Assertions.assertThat(fs.exists(new Path("a/b/c")))
        .describedAs("Path a/b/c does not exist")
        .isTrue();
    intercept(IOException.class, () -> fs.create(new Path("a/b")));

    // Asserting that directories created as explicit
    FileStatus status2 = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path("a/b/c")),
        new TracingContext(getTestTracingContext(fs, true)));
    Assertions.assertThat(status2.isDirectory())
        .describedAs("Path a/b/c is not an explicit directory")
        .isTrue();
  }

  /**
   * Test creation of directory by level.
   * @throws Exception
   */
  @Test
  public void testMkdirsByLevel() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a"));
    fs.mkdirs(new Path("a/b/c"));
    fs.mkdirs(new Path("a/b/c/d/e"));

    Assertions.assertThat(fs.exists(new Path("a")))
        .describedAs("Path a does not exist")
        .isTrue();
    Assertions.assertThat(fs.exists(new Path("a/b/c")))
        .describedAs("Path a/b/c does not exist")
        .isTrue();
    Assertions.assertThat(fs.exists(new Path("a/b/c/d/e")))
        .describedAs("Path a/b/c/d/e does not exist")
        .isTrue();

    // Asserting that directories created as explicit
    FileStatus status = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path("a/")),
        new TracingContext(getTestTracingContext(fs, true)));
    Assertions.assertThat(status.isDirectory())
        .describedAs("Path a is not an explicit directory")
        .isTrue();
    FileStatus status1 = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path("a/b/c")),
        new TracingContext(getTestTracingContext(fs, true)));
    Assertions.assertThat(status1.isDirectory())
        .describedAs("Path a/b/c is not an explicit directory")
        .isTrue();
    FileStatus status2 = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path("a/b/c/d/e")),
        new TracingContext(getTestTracingContext(fs, true)));
    Assertions.assertThat(status2.isDirectory())
        .describedAs("Path a/b/c/d/e is not an explicit directory")
        .isTrue();
  }

  /*
    Delete part of a path and validate sub path exists.
   */
  @Test
  public void testMkdirsWithDelete() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a/b"));
    fs.mkdirs(new Path("a/b/c/d"));
    fs.delete(new Path("a/b/c/d"));
    fs.getFileStatus(new Path("a/b/c"));
    Assertions.assertThat(fs.exists(new Path("a/b/c")))
        .describedAs("Path a/b/c does not exist")
        .isTrue();
  }

  /**
   * Verify mkdir and rename of parent.
   */
  @Test
  public void testMkdirsWithRename() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a/b/c/d"));
    fs.create(new Path("e/file"));
    fs.delete(new Path("a/b/c/d"));
    Assertions.assertThat(fs.rename(new Path("e"), new Path("a/b/c/d")))
        .describedAs("Failed to rename path e to a/b/c/d")
        .isTrue();
    Assertions.assertThat(fs.exists(new Path("a/b/c/d/file")))
        .describedAs("Path a/b/c/d/file does not exist")
        .isTrue();
  }

  /**
   * Create a file with name /dir1 and then mkdirs for /dir1/dir2 should fail.
   * @throws Exception
   */
  @Test
  public void testFileCreateMkdirsRoot() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.setWorkingDirectory(new Path("/"));
    final Path p1 = new Path("dir1");
    fs.create(p1);
    intercept(IOException.class, () -> fs.mkdirs(new Path("dir1/dir2")));
  }

  /**
   * Create a file with name /dir1 and then mkdirs for /dir1/dir2 should fail.
   * @throws Exception
   */
  @Test
  public void testFileCreateMkdirsNonRoot() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path p1 = new Path("dir1");
    fs.create(p1);
    intercept(IOException.class, () -> fs.mkdirs(new Path("dir1/dir2")));
  }

  /**
   * Creation of same directory without overwrite flag should pass.
   * @throws Exception
   */
  @Test
  public void testCreateSameDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a/b/c"));
    fs.mkdirs(new Path("a/b/c"));

    Assertions.assertThat(fs.exists(new Path("a/b/c")))
        .describedAs("Path a/b/c does not exist")
        .isTrue();
    // Asserting that directories created as explicit
    FileStatus status = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path("a/b/c")),
        new TracingContext(getTestTracingContext(fs, true)));
    Assertions.assertThat(status.isDirectory())
        .describedAs("Path a/b/c is not an explicit directory")
        .isTrue();
  }

  /**
   * Creation of same directory without overwrite flag should pass.
   * @throws Exception
   */
  @Test
  public void testCreateSamePathDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.create(new Path("a"));
    intercept(IOException.class, () -> fs.mkdirs(new Path("a")));
  }

  /**
   * Creation of directory with root as parent
   */
  @Test
  public void testMkdirOnRootAsParent() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path path = new Path("a");
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(path);

    // Asserting that the directory created by mkdir exists as explicit.
    FileStatus status = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path("a")),
        new TracingContext(getTestTracingContext(fs, true)));
    Assertions.assertThat(status.isDirectory())
        .describedAs("Path a is not an explicit directory")
        .isTrue();
  }

  /**
   * Creation of directory on root
   */
  @Test
  public void testMkdirOnRoot() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path path = new Path("/");
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(path);

    FileStatus status = fs.getAbfsStore().getFileStatus(fs.makeQualified(new Path("/")),
        new TracingContext(getTestTracingContext(fs, true)));
    Assertions.assertThat(status.isDirectory())
        .describedAs("Path is not an explicit directory")
        .isTrue();
  }

  /**
   * Creation of directory on path with unicode chars
   */
  @Test
  public void testMkdirUnicode() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path path = new Path("/dir\u0031");
    fs.mkdirs(path);

    // Asserting that the directory created by mkdir exists as explicit.
    FileStatus status = fs.getAbfsStore().getFileStatus(fs.makeQualified(path),
        new TracingContext(getTestTracingContext(fs, true)));
    Assertions.assertThat(status.isDirectory())
        .describedAs("Path is not an explicit directory")
        .isTrue();
  }

  /**
   * Creation of directory on same path with parallel threads.
   */
  @Test
  public void testMkdirParallelRequests() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path path = new Path("/dir1");

    ExecutorService es = Executors.newFixedThreadPool(3);

    List<CompletableFuture<Void>> tasks = new ArrayList<>();

    for (int i = 0; i < 3; i++) {
      CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
        try {
          fs.mkdirs(path);
        } catch (IOException e) {
          throw new CompletionException(e);
        }
      }, es);
      tasks.add(future);
    }

    // Wait for all the tasks to complete
    CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).join();

    // Assert that the directory created by mkdir exists as explicit
    FileStatus status = fs.getAbfsStore().getFileStatus(fs.makeQualified(path),
        new TracingContext(getTestTracingContext(fs, true)));
    Assertions.assertThat(status.isDirectory())
        .describedAs("Path is not an explicit directory")
        .isTrue();
  }


  /**
   * Creation of directory with overwrite set to false should not fail according to DFS code.
   * @throws Exception
   */
  @Test
  public void testCreateSameDirectoryOverwriteFalse() throws Exception {
    Configuration configuration = getRawConfiguration();
    configuration.setBoolean(FS_AZURE_ENABLE_MKDIR_OVERWRITE, false);
    AzureBlobFileSystem fs1 = (AzureBlobFileSystem) FileSystem.newInstance(configuration);
    fs1.mkdirs(new Path("a/b/c"));
    fs1.mkdirs(new Path("a/b/c"));

    // Asserting that directories created as explicit
    FileStatus status = fs1.getAbfsStore().getFileStatus(fs1.makeQualified(new Path("a/b/c")),
        new TracingContext(getTestTracingContext(fs1, true)));
    Assertions.assertThat(status.isDirectory())
        .describedAs("Path is not an explicit directory")
        .isTrue();
  }

  /**
   * Try creating directory same as an existing file.
   */
  @Test
  public void testCreateDirectoryAndFileRecreation() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a/b/c"));
    fs.create(new Path("a/b/c/d"));
    Assertions.assertThat(fs.exists(new Path("a/b/c")))
        .describedAs("Directory a/b/c does not exist")
        .isTrue();
    Assertions.assertThat(fs.exists(new Path("a/b/c/d")))
        .describedAs("File a/b/c/d does not exist")
        .isTrue();
    intercept(IOException.class, () -> fs.mkdirs(new Path("a/b/c/d")));
  }

  @Test
  public void testCreateNonRecursiveForAtomicDirectoryFile() throws Exception {
    AzureBlobFileSystem fileSystem = getFileSystem();
    fileSystem.setWorkingDirectory(new Path("/"));
    fileSystem.mkdirs(new Path("/hbase/dir"));
    fileSystem.createFile(new Path("/hbase/dir/file"))
        .overwrite(false)
        .replication((short) 1)
        .bufferSize(1024)
        .blockSize(1024)
        .build();
    Assertions.assertThat(fileSystem.exists(new Path("/hbase/dir/file")))
        .describedAs("File /hbase/dir/file does not exist")
        .isTrue();
  }

  @Test
  public void testMkdirOnNonExistingPathWithImplicitParentDir() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path implicitPath = new Path("dir1");
    final Path path = new Path("dir1/dir2");
    createAzCopyFolder(implicitPath);

    // Creating a directory on non-existing path inside an implicit directory
    fs.mkdirs(path);

    // Asserting that path created by azcopy becomes explicit.
    Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(implicitPath,
            fs, getTestTracingContext(fs, true)))
        .describedAs("Path created by azcopy did not become explicit")
        .isTrue();

    // Asserting that the directory created by mkdir exists as explicit.
    Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(path,
            fs, getTestTracingContext(fs, true)))
        .describedAs("Directory created by mkdir does not exist as explicit")
        .isTrue();
  }

  /**
   * Creation of directory with parent directory existing as implicit.
   * And the directory to be created existing as explicit directory
   * @throws Exception
   */
  @Test
  public void testMkdirOnExistingExplicitDirWithImplicitParentDir() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path implicitPath = new Path("dir1");
    final Path path = new Path("dir1/dir2");

    // Creating implicit directory to be used as parent
    createAzCopyFolder(implicitPath);

    // Creating an explicit directory on the path first
    fs.mkdirs(path);

    // Creating a directory on existing explicit directory inside an implicit directory
    fs.mkdirs(path);

    // Asserting that path created by azcopy becomes explicit.
    Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(implicitPath,
            fs, getTestTracingContext(fs, true)))
        .describedAs("Path created by azcopy did not become explicit")
        .isTrue();

    // Asserting that the directory created by mkdir exists as explicit.
    Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(path,
            fs, getTestTracingContext(fs, true)))
        .describedAs("Directory created by mkdir does not exist as explicit")
        .isTrue();
  }

  /**
   * Creation of directory with parent directory existing as explicit.
   * And the directory to be created existing as implicit directory
   * @throws Exception
   */
  @Test
  public void testMkdirOnExistingImplicitDirWithExplicitParentDir() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path explicitPath = new Path("dir1");
    final Path path = new Path("dir1/dir2");

    // Creating an explicit directory to be used a parent
    fs.mkdirs(explicitPath);

    createAzCopyFolder(path);

    // Creating a directory on existing implicit directory inside an explicit directory
    fs.mkdirs(path);

    // Asserting that the directory created by mkdir exists as explicit.
    Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(explicitPath,
            fs, getTestTracingContext(fs, true)))
        .describedAs("Explicit parent directory does not exist as explicit")
        .isTrue();

    // Asserting that the directory created by mkdir exists as explicit.
    Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(path,
            fs, getTestTracingContext(fs, true)))
        .describedAs("Directory created by mkdir does not exist as explicit")
        .isTrue();
  }

  /**
   * Creation of directory with parent directory existing as implicit.
   * And the directory to be created existing as implicit directory
   * @throws Exception
   */
  @Test
  public void testMkdirOnExistingImplicitDirWithImplicitParentDir() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path implicitPath = new Path("dir3");
    final Path path = new Path("dir3/dir4");

    createAzCopyFolder(implicitPath);

    // Creating an implicit directory on path
    createAzCopyFolder(path);

    // Creating a directory on existing implicit directory inside an implicit directory
    fs.mkdirs(path);

    // Asserting that path created by azcopy becomes explicit.
    Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(implicitPath,
            fs, getTestTracingContext(fs, true)))
        .describedAs("Path created by azcopy did not become explicit")
        .isTrue();

    // Asserting that the directory created by mkdir exists as explicit.
    Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(path,
            fs, getTestTracingContext(fs, true)))
        .describedAs("Directory created by mkdir does not exist as explicit")
        .isTrue();
  }

  /**
   * Creation of directory with parent directory existing as implicit.
   * And the directory to be created existing as file
   * @throws Exception
   */
  @Test
  public void testMkdirOnExistingFileWithImplicitParentDir() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path implicitPath = new Path("dir1");
    final Path path = new Path("dir1/dir2");

    createAzCopyFolder(implicitPath);

    // Creating a file on path
    fs.create(path);

    // Creating a directory on existing file inside an implicit directory
    // Asserting that the mkdir fails
    LambdaTestUtils.intercept(FileAlreadyExistsException.class, () -> {
      fs.mkdirs(path);
    });

    // Asserting that path created by azcopy becomes explicit.
    Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(implicitPath,
            fs, getTestTracingContext(fs, true)))
        .describedAs("Path created by azcopy did not become explicit")
        .isTrue();

    // Asserting that the file still exists at path.
    Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(path,
            fs, getTestTracingContext(fs, true)))
        .describedAs("File still exists at path")
        .isFalse();
  }

  /**
   * 1. a/b/c as implicit.
   * 2. Create marker for b.
   * 3. Do mkdir on a/b/c/d.
   * 4. Verify all b,c,d have marker but a is implicit.
   */
  @Test
  public void testImplicitExplicitFolder() throws Exception {
    Configuration configuration = Mockito.spy(getRawConfiguration());
    final AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(configuration);
    final Path implicitPath = new Path("a/b/c");

    createAzCopyFolder(implicitPath);

    Path path = makeQualified(new Path("a/b"));
    AbfsBlobClient blobClient = (AbfsBlobClient) fs.getAbfsStore().getClient(AbfsServiceType.BLOB);
    blobClient.createPath(path.toUri().getPath(), false, true,
        null, false, null, null, getTestTracingContext(fs, true),
        true);

    fs.mkdirs(new Path("a/b/c/d"));

    Assertions.assertThat(DirectoryStateHelper.isImplicitDirectory(new Path("a"),
            fs, getTestTracingContext(fs, true)))
        .describedAs("Directory 'a' should be implicit")
        .isTrue();
    Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(new Path("a/b"),
            fs, getTestTracingContext(fs, true)))
        .describedAs("Directory 'a/b' should be explicit")
        .isTrue();
    Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(new Path("a/b/c"),
            fs, getTestTracingContext(fs, true)))
        .describedAs("Directory 'a/b/c' should be explicit")
        .isTrue();
    Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(new Path("a/b/c/d"),
            fs, getTestTracingContext(fs, true)))
        .describedAs("Directory 'a/b/c/d' should be explicit")
        .isTrue();
  }

  /**
   * 1. a/b/c implicit.
   * 2. Marker for a and c.
   * 3. mkdir on a/b/c/d.
   * 4. Verify a,c,d are explicit but b is implicit.
   */
  @Test
  public void testImplicitExplicitFolder1() throws Exception {
    Configuration configuration = Mockito.spy(getRawConfiguration());
    final AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(configuration);
    final Path implicitPath = new Path("a/b/c");

    createAzCopyFolder(implicitPath);

    Path path = makeQualified(new Path("a"));
    AbfsBlobClient blobClient = (AbfsBlobClient) fs.getAbfsStore().getClient(AbfsServiceType.BLOB);
    blobClient.createPath(path.toUri().getPath(), false, true,
        null, false, null, null, getTestTracingContext(fs, true), true);

    Path newPath = makeQualified(new Path("a/b/c"));
    blobClient.createPath(newPath.toUri().getPath(), false, true,
        null, false, null, null, getTestTracingContext(fs, true), true);

    fs.mkdirs(new Path("a/b/c/d"));

    Assertions.assertThat(DirectoryStateHelper.isImplicitDirectory(new Path("a/b"),
            fs, getTestTracingContext(fs, true)))
        .describedAs("Directory 'a/b' should be implicit")
        .isTrue();

    // Asserting that the directory created by mkdir exists as explicit.
    Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(new Path("a"),
            fs, getTestTracingContext(fs, true)))
        .describedAs("Directory 'a' should be explicit")
        .isTrue();
    Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(new Path("a/b/c"),
            fs, getTestTracingContext(fs, true)))
        .describedAs("Directory 'a/b/c' should be explicit")
        .isTrue();
    Assertions.assertThat(DirectoryStateHelper.isExplicitDirectory(new Path("a/b/c/d"),
            fs, getTestTracingContext(fs, true)))
        .describedAs("Directory 'a/b/c/d' should be explicit")
        .isTrue();
  }

  /**
   * Extracts the eTag for an existing file
   * @param fileName file Path in String from container root
   * @return String etag for the file
   * @throws IOException
   */
  private String extractFileEtag(String fileName) throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    final AbfsClient client = fs.getAbfsClient();
    final TracingContext testTracingContext = getTestTracingContext(fs, false);
    AbfsRestOperation op;
    op = client.getPathStatus(fileName, true, testTracingContext, null);
    return AzureBlobFileSystemStore.extractEtagHeader(op.getResult());
  }
}
