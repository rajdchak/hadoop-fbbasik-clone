package org.apache.hadoop.fs.s3a;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSInputStream;

import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class S3ASeekableStream extends FSInputStream {

    private S3SeekableInputStream inputStream;
    private final String key;

    public static final Logger LOG = LoggerFactory.getLogger(S3ASeekableStream.class);


    public S3ASeekableStream(String bucket, String key, S3SeekableInputStreamFactory s3SeekableInputStreamFactory)
            throws IOException {
        this.inputStream = s3SeekableInputStreamFactory.createStream(S3URI.of(bucket, key));
        this.key = key;
    }

    @Override
    public int read() throws IOException {
        return inputStream.read();
    }

    @Override
    public void seek(long pos) throws IOException {
        inputStream.seek(pos);
    }

    @Override
    public long getPos() throws IOException {
        return inputStream.getPos();
    }

    @Override
    public void close() throws IOException {
        if (inputStream != null) {
            inputStream.close();
            inputStream = null;
            super.close();
        }
    }


    public void readTail(byte[] buf, int off, int n) throws IOException {
        inputStream.readTail(buf, off, n);
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        return inputStream.read(buf, off, len);
    }


    @Override
    public boolean seekToNewSource(long l) throws IOException {
        return false;
    }

}