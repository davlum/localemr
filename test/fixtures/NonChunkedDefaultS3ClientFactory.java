/*
MIT License

Copyright (c) 2019 Branden Smith

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

https://github.com/sumitsu/s3_mocktest_demo/blob/master/src/test/java/dev/sumitsu/s3mocktest/testutil/NonChunkedDefaultS3ClientFactory.java
*/

package dev.sumitsu.s3mocktest.testutil;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.S3ClientOptions;
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory;

import java.io.IOException;
import java.net.URI;

public class NonChunkedDefaultS3ClientFactory extends DefaultS3ClientFactory {
  @Override
  public AmazonS3 createS3Client(final URI name, final String bucket, final AWSCredentialsProvider credentials) throws IOException {
    final AmazonS3 s3 = super.createS3Client(name, bucket, credentials);
    s3.setS3ClientOptions(
      S3ClientOptions.builder()
        .disableChunkedEncoding()
        .setPathStyleAccess(true)
        .build());
    return s3;
  }
}