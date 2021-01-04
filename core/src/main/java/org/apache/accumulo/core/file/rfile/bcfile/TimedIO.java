/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.file.rfile.bcfile;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.accumulo.core.util.RegionTimer;
import org.apache.accumulo.core.util.TimerManager;
import org.apache.hadoop.io.compress.BlockCompressorStream;
import org.apache.hadoop.io.compress.BlockDecompressorStream;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.CompressorStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DecompressorStream;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.ZStandardCodec;

/*---------------------------------------------------------------------------
 * things added for stats gathering.  NOT FOR PRODUCTION.
 * --------------------------------------------------------------------------
 */
public class TimedIO {
  private static final String KEY_COMPRESS = "compress";
  private static final String KEY_DECOMP = "decompress";
  private static final String KEY_DECOMP_READ = "decompRead";
  private static final String KEY_GET_COMP_DATA = "readFromHdfs";
  private static final String KEY_WRITE_COMP = "writeCompressed";
  private static final String KEY_WRITE_TO_COMP = "writeToCompressor";
  private static final String KEY_WRITE_TO_HDFS = "writeToHdfs";

  // set on command line: -Daccumulo.timing=true
  static final boolean isTiming = TimerManager.isTiming();

  static class TimedSimpleBufferedOutputStream extends SimpleBufferedOutputStream {
    public TimedSimpleBufferedOutputStream(OutputStream out, byte[] buf) {
      super(out, buf);
    }

    @Override
    protected void flushBuffer() throws IOException {
      if (count > 0) {
        RegionTimer timer = TimerManager.timerForThread();
        timer.enter(TimedIO.KEY_WRITE_TO_HDFS);
        try {
          out.write(buf, 0, count);
          count = 0;
        } finally {
          timer.exit(TimedIO.KEY_WRITE_TO_HDFS);
        }
      }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      if (len >= buf.length) {
        RegionTimer timer = TimerManager.timerForThread();
        timer.enter(TimedIO.KEY_WRITE_TO_HDFS);
        try {
          flushBuffer();
          out.write(b, off, len);
          return;
        } finally {
          timer.exit(TimedIO.KEY_WRITE_TO_HDFS);
        }
      }
      if (len > buf.length - count) {
        flushBuffer();
      }
      System.arraycopy(b, off, buf, count, len);
      count += len;
    }
  }

  static class TimedInputStream extends FilterInputStream {
    TimedInputStream(InputStream is) {
      super(is);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      RegionTimer timer = TimerManager.timerForThread();
      timer.enter(KEY_GET_COMP_DATA);
      try {
        return super.read(b, off, len);
      } finally {
        timer.exit(KEY_GET_COMP_DATA);
      }
    }
  }

  static class TimedDecompressionStream extends FilterInputStream {
    TimedDecompressionStream(CompressionInputStream is) {
      super(is);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      RegionTimer timer = TimerManager.timerForThread();
      timer.enter(KEY_DECOMP_READ);
      try {
        return in.read(b, off, len);
      } finally {
        timer.exit(KEY_DECOMP_READ);
      }
    }
  }

  static class TimedFinishOnFlushCompressionStream
      extends Compression.FinishOnFlushCompressionStream {
    TimedFinishOnFlushCompressionStream(CompressionOutputStream cout) {
      super(cout);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      RegionTimer timer = TimerManager.timerForThread();
      timer.enter(KEY_WRITE_TO_COMP);
      try {
        super.write(b, off, len);
      } finally {
        timer.exit(KEY_WRITE_TO_COMP);
      }
    }

    @Override
    public void flush() throws IOException {
      RegionTimer timer = TimerManager.timerForThread();
      timer.enter(KEY_WRITE_TO_COMP);
      try {
        super.flush();
      } finally {
        timer.exit(KEY_WRITE_TO_COMP);
      }
    }
  }

  // GZ
  public static class TimedGZCodec extends DefaultCodec {
    @Override
    public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor)
        throws IOException {
      return newTimedDecompressorStream(in, decompressor,
          getConf().getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT));
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor) {
      return newTimedCompressorStream(out, compressor,
          this.getConf().getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT));
    }
  }

  // ZSTD: org.apache.accumulo.core.file.rfile.bcfile.TimedCompression$TimedZStandardCodec
  static final String TIMED_ZSTD_CODEC = TimedZStandardCodec.class.getName();

  public static class TimedZStandardCodec extends ZStandardCodec {
    @Override
    public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor)
        throws IOException {
      checkNativeCodeLoaded();
      return newTimedDecompressorStream(in, decompressor,
          getDecompressionBufferSize(this.getConf()));
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor) {
      checkNativeCodeLoaded();
      return newTimedCompressorStream(out, compressor, getCompressionBufferSize(this.getConf()));
    }
  }

  // Snappy: org.apache.accumulo.core.file.rfile.bcfile.TimedCompression$TimedSnappyCodec
  static final String TIMED_SNAPPY_CODEC = TimedSnappyCodec.class.getName();

  public static class TimedSnappyCodec extends SnappyCodec {
    @Override
    public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor)
        throws IOException {
      checkNativeCodeLoaded();
      return new BlockDecompressorStream(in, decompressor,
          this.getConf().getInt("io.compression.codec.snappy.buffersize", 262144)) {
        @Override
        public int read(byte[] b, int off, int len) throws IOException {
          checkStream();

          if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
          } else if (len == 0) {
            return 0;
          }

          RegionTimer timer = TimerManager.timerForThread();
          timer.enter(KEY_DECOMP);
          try {
            return decompress(b, off, len);
          } finally {
            timer.exit(KEY_DECOMP);
          }
        }

        @Override
        protected int getCompressedData() throws IOException {
          RegionTimer timer = TimerManager.timerForThread();
          timer.enter(KEY_GET_COMP_DATA);
          try {
            return super.getCompressedData();
          } finally {
            timer.exit(KEY_GET_COMP_DATA);
          }
        }
      };
    }

    @Override
    public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor) {
      checkNativeCodeLoaded();
      int bufferSize = this.getConf().getInt("io.compression.codec.snappy.buffersize", 262144);
      int compressionOverhead = bufferSize / 6 + 32;
      return new BlockCompressorStream(out, compressor, bufferSize, compressionOverhead) {
        @Override
        protected void compress() throws IOException {
          RegionTimer timer = TimerManager.timerForThread();
          timer.enter(KEY_COMPRESS);
          int len;
          try {
            len = this.compressor.compress(this.buffer, 0, this.buffer.length);
          } finally {
            timer.exit(KEY_COMPRESS);
          }
          if (len > 0) {
            timer.enter(KEY_WRITE_COMP);
            try {
              this.rawWriteInt(len);
              this.out.write(this.buffer, 0, len);
            } finally {
              timer.exit(KEY_WRITE_COMP);
            }
          }
        }

        private void rawWriteInt(int v) throws IOException {
          this.out.write(v >>> 24 & 255);
          this.out.write(v >>> 16 & 255);
          this.out.write(v >>> 8 & 255);
          this.out.write(v & 255);
        }
      };
    }
  }

  private static CompressionOutputStream newTimedCompressorStream(OutputStream out,
      Compressor compressor, int bufferSize) {
    return new CompressorStream(out, compressor, bufferSize) {
      @Override
      protected void compress() throws IOException {
        RegionTimer timer = TimerManager.timerForThread();
        int len;
        timer.enter(KEY_COMPRESS);
        try {
          len = compressor.compress(buffer, 0, buffer.length);
        } finally {
          timer.exit(KEY_COMPRESS);
        }

        if (len > 0) {
          timer.enter(KEY_WRITE_COMP);
          try {
            out.write(buffer, 0, len);
          } finally {
            timer.exit(KEY_WRITE_COMP);
          }
        }
      }
    };
  }

  private static DecompressorStream newTimedDecompressorStream(InputStream in,
      Decompressor decompressor, int bufferSize) throws IOException {
    return new DecompressorStream(in, decompressor, bufferSize) {
      @Override
      protected int getCompressedData() throws IOException {
        RegionTimer timer = TimerManager.timerForThread();
        timer.enter(KEY_GET_COMP_DATA);
        try {
          return super.getCompressedData();
        } finally {
          timer.exit(KEY_GET_COMP_DATA);
        }
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        checkStream();

        if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
          throw new IndexOutOfBoundsException();
        } else if (len == 0) {
          return 0;
        }

        RegionTimer timer = TimerManager.timerForThread();
        timer.enter(KEY_DECOMP);
        try {
          return decompress(b, off, len);
        } finally {
          timer.exit(KEY_DECOMP);
        }
      }
    };
  }
}
