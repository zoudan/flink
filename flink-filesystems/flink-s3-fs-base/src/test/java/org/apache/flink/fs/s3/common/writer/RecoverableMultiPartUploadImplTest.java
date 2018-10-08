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

package org.apache.flink.fs.s3.common.writer;

import org.apache.flink.fs.s3.common.utils.RefCountedBufferingFileStream;
import org.apache.flink.fs.s3.common.utils.RefCountedFile;
import org.apache.flink.util.MathUtils;

import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for the {@link RecoverableMultiPartUploadImpl}.
 */
public class RecoverableMultiPartUploadImplTest {

	private static final int BUFFER_SIZE = 10;

	private static final String TEST_OBJECT_NAME = "TEST-OBJECT";

	private TestMultiPartUploader uploader;

	private RecoverableMultiPartUploadImpl multiPartUpload;

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Before
	public void initializeUpload() throws IOException {
		uploader = new TestMultiPartUploader();
		multiPartUpload = RecoverableMultiPartUploadImpl
				.newUpload(uploader, new MainThreadExecutor(), TEST_OBJECT_NAME);
	}

	@Test
	public void normalUploadShouldSucceed() throws IOException {
		final byte[] smallContent = bytesOf("hello world");

		final TestUploadPartResult expectedUploadPartResult =
				getUploadPartResult(TEST_OBJECT_NAME, 1, smallContent);

		uploadCompletePart(smallContent);
		assertThat(uploader, hasCompleteParts(expectedUploadPartResult));
	}

	@Test
	public void snapshotWithIncompletePartShouldSucceed() throws IOException {
		byte[] incompletePart = bytesOf("Hi!");

		final TestPutObjectResult expectedIncompletePartResult =
				getPutObjectResult(TEST_OBJECT_NAME, incompletePart);

		uploadIncompletePart(incompletePart);
		assertThat(uploader, hasIncompleteParts(expectedIncompletePartResult));
	}

	@Test
	public void snapshotAndGetRecoverableShouldSucceed() throws IOException {
		final byte[] firstCompletePart = bytesOf("hello world");
		final byte[] secondCompletePart = bytesOf("hello again");
		final byte[] thirdIncompletePart = bytesOf("!!!");

		uploadCompletePart(firstCompletePart);
		uploadCompletePart(secondCompletePart);
		final S3Recoverable recoverable = uploadIncompletePart(thirdIncompletePart);

		assertThat(uploader,
				Matchers.allOf(
						hasCompleteParts(
								getUploadPartResult(TEST_OBJECT_NAME, 1, firstCompletePart),
								getUploadPartResult(TEST_OBJECT_NAME, 2, secondCompletePart)),
						hasIncompleteParts(
								getPutObjectResult(TEST_OBJECT_NAME, thirdIncompletePart))
				)
		);

		final S3Recoverable expectedRecoverable =
				createS3Recoverable(thirdIncompletePart, firstCompletePart, secondCompletePart);

		assertThat(recoverable, isEqualTo(expectedRecoverable));
	}

	@Test(expected = IllegalStateException.class)
	public void uploadingNonClosedFileAsCompleteShouldThroughException() throws IOException {
		final byte[] incompletePart = bytesOf("!!!");

		final RefCountedBufferingFileStream incompletePartFile =
				getOpenTestFileWithContent(incompletePart);

		multiPartUpload.uploadPart(incompletePartFile);
	}

	// --------------------------------- Matchers ---------------------------------

	private static TypeSafeMatcher<RefCountedBufferingFileStream> hasSingleReference() {
		return new TypeSafeMatcher<RefCountedBufferingFileStream>() {
			@Override
			protected boolean matchesSafely(RefCountedBufferingFileStream file) {
				return file.getReferenceCounter() == 1;
			}

			@Override
			public void describeTo(Description description) {
				description
						.appendText("Reference Counted File with reference counter=")
						.appendValue(1);
			}
		};
	}

	private static TypeSafeMatcher<TestMultiPartUploader> hasCompleteParts(final TestUploadPartResult... expectedCompleteParts) {
		return new TypeSafeMatcher<TestMultiPartUploader>() {
			@Override
			protected boolean matchesSafely(TestMultiPartUploader testMultipartUploader) {
				final List<TestUploadPartResult> actualCompleteParts =
						testMultipartUploader.getCompletePartsUploaded();

				return Arrays.equals(expectedCompleteParts, actualCompleteParts.toArray());
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("a TestMultiPartUploader with complete parts=");
				for (TestUploadPartResult part : expectedCompleteParts) {
						description.appendValue(part + " ");
				}
			}
		};
	}

	private static TypeSafeMatcher<TestMultiPartUploader> hasIncompleteParts(final TestPutObjectResult... expectedIncompleteParts) {
		return new TypeSafeMatcher<TestMultiPartUploader>() {
			@Override
			protected boolean matchesSafely(TestMultiPartUploader testMultipartUploader) {
				final List<TestPutObjectResult> actualIncompleteParts =
						testMultipartUploader.getIncompletePartsUploaded();

				return Arrays.equals(expectedIncompleteParts, actualIncompleteParts.toArray());
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("a TestMultiPartUploader with complete parts=");
				for (TestPutObjectResult part : expectedIncompleteParts) {
					description.appendValue(part + " ");
				}
			}
		};
	}

	private static TypeSafeMatcher<S3Recoverable> isEqualTo(final S3Recoverable expectedRecoverable) {
		return new TypeSafeMatcher<S3Recoverable>() {

			@Override
			protected boolean matchesSafely(S3Recoverable actualRecoverable) {

				return Objects.equals(expectedRecoverable.getObjectName(), actualRecoverable.getObjectName())
						&& Objects.equals(expectedRecoverable.uploadId(), actualRecoverable.uploadId())
						&& expectedRecoverable.numBytesInParts() == actualRecoverable.numBytesInParts()
						&& expectedRecoverable.incompleteObjectLength() == actualRecoverable.incompleteObjectLength()
						&& compareLists(expectedRecoverable.parts(), actualRecoverable.parts());
			}

			private boolean compareLists(final List<PartETag> first, final List<PartETag> second) {
				return Arrays.equals(
						first.stream().map(tag -> tag.getETag()).toArray(),
						second.stream().map(tag -> tag.getETag()).toArray()
				);
			}

			@Override
			public void describeTo(Description description) {
				description.appendText(expectedRecoverable + " with ignored LAST_PART_OBJECT_NAME.");
			}
		};
	}

	// ---------------------------------- Test Methods -------------------------------------------

	private static byte[] bytesOf(String str) {
		return str.getBytes(StandardCharsets.UTF_8);
	}

	private static S3Recoverable createS3Recoverable(byte[] incompletePart, byte[]... completeParts) {
		final List<PartETag> eTags = new ArrayList<>();

		int index = 1;
		long bytesInPart = 0L;
		for (byte[] part : completeParts) {
			eTags.add(new PartETag(index, createETag(TEST_OBJECT_NAME, index)));
			bytesInPart += part.length;
			index++;
		}

		return new S3Recoverable(
				TEST_OBJECT_NAME,
				createMPUploadId(TEST_OBJECT_NAME),
				eTags,
				bytesInPart,
				"IGNORED-DUE-TO-RANDOMNESS",
				(long) incompletePart.length);
	}

	private static RecoverableMultiPartUploadImplTest.TestPutObjectResult getPutObjectResult(String key, byte[] content) {
		final RecoverableMultiPartUploadImplTest.TestPutObjectResult result = new RecoverableMultiPartUploadImplTest.TestPutObjectResult();
		result.setETag(createETag(key, -1));
		result.setContent(content);
		return result;
	}

	private static RecoverableMultiPartUploadImplTest.TestUploadPartResult getUploadPartResult(String key, int number, byte[] payload) {
		final RecoverableMultiPartUploadImplTest.TestUploadPartResult result = new RecoverableMultiPartUploadImplTest.TestUploadPartResult();
		result.setETag(createETag(key, number));
		result.setPartNumber(number);
		result.setContent(payload);
		return result;
	}

	private static String createMPUploadId(String key) {
		return "MPU-" + key;
	}

	private static String createETag(String key, int partNo) {
		return "ETAG-" + key + '-' + partNo;
	}

	private S3Recoverable uploadIncompletePart(byte[] content) throws IOException {
		final RefCountedBufferingFileStream incompletePartFile = getOpenTestFileWithContent(content);
		incompletePartFile.flush();

		final S3Recoverable recoverable = multiPartUpload.snapshotAndGetRecoverable(incompletePartFile);
		assertThat(incompletePartFile, hasSingleReference());
		return recoverable;
	}

	private void uploadCompletePart(final byte[] content) throws IOException {
		final RefCountedBufferingFileStream partFile = getOpenTestFileWithContent(content);
		partFile.close();

		multiPartUpload.uploadPart(partFile);
		assertThat(partFile, hasSingleReference());
	}

	private RefCountedBufferingFileStream getOpenTestFileWithContent(byte[] content) throws IOException {
		final File newFile = new File(temporaryFolder.getRoot(), ".tmp_" + UUID.randomUUID());
		final OutputStream out = Files.newOutputStream(newFile.toPath(), StandardOpenOption.CREATE_NEW);

		final RefCountedBufferingFileStream testStream =
				new RefCountedBufferingFileStream(RefCountedFile.newFile(newFile, out), BUFFER_SIZE);

		testStream.write(content, 0, content.length);
		return testStream;
	}

	// ---------------------------------- Test Classes -------------------------------------------

	private static class MainThreadExecutor implements Executor {

		@Override
		public void execute(Runnable command) {
			command.run();
		}
	}

	private static class TestMultiPartUploader implements S3MultiPartUploader {

		private final List<RecoverableMultiPartUploadImplTest.TestUploadPartResult> completePartsUploaded = new ArrayList<>();
		private final List<RecoverableMultiPartUploadImplTest.TestPutObjectResult> incompletePartsUploaded = new ArrayList<>();

		public List<RecoverableMultiPartUploadImplTest.TestUploadPartResult> getCompletePartsUploaded() {
			return completePartsUploaded;
		}

		public List<RecoverableMultiPartUploadImplTest.TestPutObjectResult> getIncompletePartsUploaded() {
			return incompletePartsUploaded;
		}

		@Override
		public String startMultiPartUpload(String key) throws IOException {
			return createMPUploadId(key);
		}

		@Override
		public UploadPartResult uploadPart(String key, String uploadId, int partNumber, InputStream file, long length) throws IOException {
			final byte[] content = getFileContentBytes(file, MathUtils.checkedDownCast(length));
			return storeAndGetUploadPartResult(key, partNumber, content);
		}

		@Override
		public PutObjectResult uploadIncompletePart(String key, InputStream file, long length) throws IOException {
			final byte[] content = getFileContentBytes(file, MathUtils.checkedDownCast(length));
			return storeAndGetPutObjectResult(key, content);
		}

		@Override
		public CompleteMultipartUploadResult commitMultiPartUpload(String key, String uploadId, List<PartETag> partETags, long length, AtomicInteger errorCount) throws IOException {
			return null;
		}

		@Override
		public ObjectMetadata getObjectMetadata(String key) throws IOException {
			return null;
		}

		private byte[] getFileContentBytes(InputStream file, int length) throws IOException {
			final byte[] content = new byte[length];
			file.read(content, 0, length);
			return content;
		}

		private RecoverableMultiPartUploadImplTest.TestUploadPartResult storeAndGetUploadPartResult(String key, int number, byte[] payload) {
			final RecoverableMultiPartUploadImplTest.TestUploadPartResult result = getUploadPartResult(key, number, payload);
			completePartsUploaded.add(result);
			return result;
		}

		private RecoverableMultiPartUploadImplTest.TestPutObjectResult storeAndGetPutObjectResult(String key, byte[] payload) {
			final RecoverableMultiPartUploadImplTest.TestPutObjectResult result = getPutObjectResult(key, payload);
			incompletePartsUploaded.add(result);
			return result;
		}
	}

	private static class TestPutObjectResult extends PutObjectResult {
		private static final long serialVersionUID = 1L;

		private byte[] content;

		void setContent(byte[] payload) {
			this.content = payload;
		}

		public byte[] getContent() {
			return content;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			final TestPutObjectResult that = (TestPutObjectResult) o;
			// we ignore the etag as it contains randomness
			return Arrays.equals(getContent(), that.getContent());
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(getContent());
		}

		@Override
		public String toString() {
			return '{' +
					" eTag=" + getETag() +
					", payload=" + Arrays.toString(content) +
					'}';
		}
	}

	private static class TestUploadPartResult extends UploadPartResult {

		private static final long serialVersionUID = 1L;

		private byte[] content;

		void setContent(byte[] content) {
			this.content = content;
		}

		public byte[] getContent() {
			return content;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			final TestUploadPartResult that = (TestUploadPartResult) o;
			return getETag().equals(that.getETag())
					&& getPartNumber() == that.getPartNumber()
					&& Arrays.equals(content, that.content);
		}

		@Override
		public int hashCode() {
			return 31 * Objects.hash(getETag(), getPartNumber()) + Arrays.hashCode(getContent());
		}

		@Override
		public String toString() {
			return '{' +
					"etag=" + getETag() +
					", partNo=" + getPartNumber() +
					", content=" + Arrays.toString(content) +
					'}';
		}
	}
}
