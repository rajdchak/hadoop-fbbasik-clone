package org.apache.hadoop.fs.s3a;

import java.io.IOException;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSInputStream;

import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class S3ASeekableStream extends FSInputStream {

    private S3SeekableInputStream inputStream;
    private long lastReadCurrentPos = 0;
    private final String key;

    public static final Logger LOG = LoggerFactory.getLogger(S3ASeekableStream.class);

    public S3ASeekableStream(String bucket, String key, S3SeekableInputStreamFactory s3SeekableInputStreamFactory) {
        this.inputStream = s3SeekableInputStreamFactory.createStream(S3URI.of(bucket, key));
        this.key = key;
    }

    @Override
    public int read() throws IOException {
        throwIfClosed();
        return inputStream.read();
    }

    @Override
    public void seek(long pos) throws IOException {
        throwIfClosed();
        inputStream.seek(pos);
    }


    @Override
    public synchronized long getPos() {
        if (!isClosed()) {
            lastReadCurrentPos = inputStream.getPos();
        }
        return lastReadCurrentPos;
    }


    public void readTail(byte[] buf, int off, int n) throws IOException {
        throwIfClosed();
        inputStream.readTail(buf, off, n);
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        throwIfClosed();
        return inputStream.read(buf, off, len);
    }


    @Override
    public boolean seekToNewSource(long l) throws IOException {
        return false;
    }

    @Override
    public void close() throws IOException {
        if (inputStream != null) {
            inputStream.close();
            inputStream = null;
            super.close();
        }
    }

    protected void throwIfClosed() throws IOException {
        if (isClosed()) {
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
    }

    protected boolean isClosed() {
        return inputStream == null;
    }
}