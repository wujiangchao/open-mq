

package com.open.openmq.common.compression;

import java.io.IOException;

public interface Compressor {

    /**
     * Compress message by different compressor.
     *
     * @param src bytes ready to compress
     * @param level compression level used to balance compression rate and time consumption
     * @return compressed byte data
     * @throws IOException
     */
    byte[] compress(byte[] src, int level) throws IOException;

    /**
     * Decompress message by different compressor.
     *
     * @param src bytes ready to decompress
     * @return decompressed byte data
     * @throws IOException
     */
    byte[] decompress(byte[] src) throws IOException;
}
