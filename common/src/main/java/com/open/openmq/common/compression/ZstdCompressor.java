

package com.open.openmq.common.compression;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import com.open.openmq.common.constant.LoggerName;
import com.open.openmq.logging.InternalLogger;
import com.open.openmq.logging.InternalLoggerFactory;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ZstdCompressor implements Compressor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    @Override
    public byte[] compress(byte[] src, int level) throws IOException {
        byte[] result = src;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);
        ZstdOutputStream outputStream = new ZstdOutputStream(byteArrayOutputStream, level);
        try {
            outputStream.write(src);
            outputStream.flush();
            outputStream.close();
            result = byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            log.error("Failed to compress data by zstd", e);
            throw e;
        } finally {
            try {
                byteArrayOutputStream.close();
            } catch (IOException ignored) {
            }
        }
        return result;
    }

    @Override
    public byte[] decompress(byte[] src) throws IOException {
        byte[] result = src;
        byte[] uncompressData = new byte[src.length];
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(src);
        ZstdInputStream zstdInputStream = new ZstdInputStream(byteArrayInputStream);
        ByteArrayOutputStream resultOutputStream = new ByteArrayOutputStream(src.length);

        try {
            while (true) {
                int len = zstdInputStream.read(uncompressData, 0, uncompressData.length);
                if (len <= 0) {
                    break;
                }
                resultOutputStream.write(uncompressData, 0, len);
            }
            resultOutputStream.flush();
            resultOutputStream.close();
            result = resultOutputStream.toByteArray();
        } catch (IOException e) {
            throw e;
        } finally {
            try {
                zstdInputStream.close();
                byteArrayInputStream.close();
            } catch (IOException e) {
                log.warn("Failed to close the zstd compress stream", e);
            }
        }

        return result;
    }
}