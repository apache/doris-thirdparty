/*
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

package org.apache.hadoop.fs.s3a.impl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Collections;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import software.amazon.awssdk.awscore.retry.conditions.RetryOnErrorCodeCondition;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.retry.RetryPolicyContext;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.encryption.s3.S3EncryptionClientException;

import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.auth.NoAwsCredentialsException;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.fs.s3a.impl.ErrorTranslation.maybeExtractIOException;
import static org.apache.hadoop.fs.s3a.impl.ErrorTranslation.maybeProcessEncryptionClientException;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Unit tests related to the {@link ErrorTranslation} class.
 */
public class TestErrorTranslation extends AbstractHadoopTestBase {

  /**
   * Create an sdk exception with the given cause.
   * @param message error message
   * @param cause cause
   * @return a new exception
   */
  private static SdkClientException sdkException(
      String message,
      Throwable cause) {
    return SdkClientException.builder()
        .message(message)
        .cause(cause)
        .build();
  }

  @Test
  public void testUnknownHostExceptionExtraction() throws Throwable {
    final SdkClientException thrown = sdkException("top",
        sdkException("middle",
            new UnknownHostException("bottom")));
    final IOException ioe = intercept(UnknownHostException.class, "top",
        () -> {
          throw maybeExtractIOException("", thrown, "");
        });

    // the wrapped exception is the top level one: no stack traces have
    // been lost
    if (ioe.getCause() != thrown) {
      throw new AssertionError("Cause of " + ioe + " is not " + thrown, thrown);
    }

  }

  @Test
  public void testNoRouteToHostExceptionExtraction() throws Throwable {
    intercept(NoRouteToHostException.class, "top",
        () -> {
          throw maybeExtractIOException("p2",
              sdkException("top",
                  sdkException("middle",
                      new NoRouteToHostException("bottom"))), null);
        });
  }

  @Test
  public void testConnectExceptionExtraction() throws Throwable {
    intercept(ConnectException.class, "top",
        () -> {
          throw maybeExtractIOException("p1",
              sdkException("top",
                  sdkException("middle",
                      new ConnectException("bottom"))), null);
        });
  }

  /**
   * When there is an UncheckedIOException, its inner class is
   * extracted.
   */
  @Test
  public void testUncheckedIOExceptionExtraction() throws Throwable {
    intercept(SocketTimeoutException.class, "top",
        () -> {
          final SdkClientException thrown = sdkException("top",
              sdkException("middle",
                  new UncheckedIOException(
                      new SocketTimeoutException("bottom"))));
          throw maybeExtractIOException("p1",
              new NoAwsCredentialsException("IamProvider", thrown.toString(), thrown), null);
        });
  }

  @Test
  public void testNoConstructorExtraction() throws Throwable {
    intercept(PathIOException.class, NoConstructorIOE.MESSAGE,
        () -> {
          throw maybeExtractIOException("p1",
              sdkException("top",
                  sdkException("middle",
                      new NoConstructorIOE())), null);
        });
  }

  @Test
  public void testEncryptionClientExceptionExtraction() throws Throwable {
    intercept(NoSuchKeyException.class, () -> {
      throw maybeProcessEncryptionClientException(
          new S3EncryptionClientException("top",
              new S3EncryptionClientException("middle", NoSuchKeyException.builder().build())));
    });
  }

  @Test
  public void testNonEncryptionClientExceptionExtraction() throws Throwable {
    intercept(SdkException.class, () -> {
      throw maybeProcessEncryptionClientException(
          sdkException("top", sdkException("middle", NoSuchKeyException.builder().build())));
    });
  }

  @Test
  public void testEncryptionClientExceptionExtractionWithRTE() throws Throwable {
    intercept(S3EncryptionClientException.class, () -> {
      throw maybeProcessEncryptionClientException(
          new S3EncryptionClientException("top", new UnsupportedOperationException()));
    });
  }



    public static final class NoConstructorIOE extends IOException {

    public static final String MESSAGE = "no-arg constructor";

    public NoConstructorIOE() {
      super(MESSAGE);
    }
  }


  @Test
  public void testMultiObjectExceptionFilledIn() throws Throwable {

    MultiObjectDeleteException ase =
        new MultiObjectDeleteException(Collections.emptyList());
    RetryPolicyContext context = RetryPolicyContext.builder()
        .exception(ase)
        .build();
    RetryOnErrorCodeCondition retry = RetryOnErrorCodeCondition.create("");

    Assertions.assertThat(retry.shouldRetry(context))
        .describedAs("retry policy of MultiObjectException")
        .isFalse();
  }
}
