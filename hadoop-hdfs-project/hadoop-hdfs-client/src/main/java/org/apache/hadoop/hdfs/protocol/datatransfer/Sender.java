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
package org.apache.hadoop.hdfs.protocol.datatransfer;

import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil.toProto;

import java.io.DataOutput;
import java.io.IOException;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.CachingStrategyProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ChecksumProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientOperationHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.DataTransferTraceInfoProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockChecksumProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpCopyBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpReadBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpReplaceBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpRequestShortCircuitAccessProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpTransferBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpWriteBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ReleaseShortCircuitAccessRequestProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ShortCircuitShmRequestProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelperClient;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.hdfs.shortcircuit.ShortCircuitShm.SlotId;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;

import org.apache.htrace.core.SpanId;
import org.apache.htrace.core.Tracer;


import com.google.protobuf.Message;

/** Sender */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class Sender implements DataTransferProtocol {
  private final DataOutputStream out;
  public static HProf hprof;


  /** Create a sender for DataTransferProtocol with a output stream. */
  public Sender(final DataOutputStream out) {
    this.out = out;
    hprof = new HProf("Sender");
  }

  /** Initialize a operation. */
  private static void op(final DataOutput out, final Op op) throws IOException {
    out.writeShort(DataTransferProtocol.DATA_TRANSFER_VERSION);
    op.write(out);
  }

  private static void send(final DataOutputStream out, final Op opcode,
      final Message proto) throws IOException {
    // LOG.trace("Sending DataTransferOp {}: {}",
        // proto.getClass().getSimpleName(), proto);
    op(out, opcode);
    proto.writeDelimitedTo(out);
    out.flush();
  }

  static private CachingStrategyProto getCachingStrategy(
      CachingStrategy cachingStrategy) {
    CachingStrategyProto.Builder builder = CachingStrategyProto.newBuilder();
    if (cachingStrategy.getReadahead() != null) {
      builder.setReadahead(cachingStrategy.getReadahead());
    }
    if (cachingStrategy.getDropBehind() != null) {
      builder.setDropBehind(cachingStrategy.getDropBehind());
    }
    return builder.build();
  }

  @Override
  public void readBlock(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken,
      final String clientName,
      final long blockOffset,
      final long length,
      final boolean sendChecksum,
      final CachingStrategy cachingStrategy) throws IOException {

    StringBuilder sb = new StringBuilder();
    sb.append("B{");
    sb.append(blk.getBlockPoolId());
    sb.append(" | ");
    sb.append(blk.getBlockId());
    sb.append(" | ");
    sb.append(blk.getGenerationStamp());
    sb.append(" | ");
    sb.append(blk.getNumBytes());
    sb.append("}");
    sb.append(" | ");
    sb.append(clientName);
    sb.append(" | ");
    sb.append(blockOffset);
    sb.append(" | ");
    sb.append(length);
    sb.append(" | ");
    sb.append(cachingStrategy.toString());


    hprof.writeLogMessage(HProf.MessageType.DATA, "readBlock", sb.toString());
    // LOG.info("Hprof: DATA readBlock "+sb.toString());
    // LOG.info("Hprof: DATA readBlock ...");

    OpReadBlockProto proto = OpReadBlockProto.newBuilder()
        .setHeader(DataTransferProtoUtil.buildClientHeader(blk, clientName,
            blockToken))
        .setOffset(blockOffset)
        .setLen(length)
        .setSendChecksums(sendChecksum)
        .setCachingStrategy(getCachingStrategy(cachingStrategy))
        .build();

    send(out, Op.READ_BLOCK, proto);
  }


  @Override
  public void writeBlock(final ExtendedBlock blk,
      final StorageType storageType,
      final Token<BlockTokenIdentifier> blockToken,
      final String clientName,
      final DatanodeInfo[] targets,
      final StorageType[] targetStorageTypes,
      final DatanodeInfo source,
      final BlockConstructionStage stage,
      final int pipelineSize,
      final long minBytesRcvd,
      final long maxBytesRcvd,
      final long latestGenerationStamp,
      DataChecksum requestedChecksum,
      final CachingStrategy cachingStrategy,
      final boolean allowLazyPersist,
      final boolean pinning,
      final boolean[] targetPinnings) throws IOException {


    StringBuilder sb = new StringBuilder();
    sb.append("B{");
    sb.append(blk.getBlockPoolId());
    sb.append(" | ");
    sb.append(blk.getBlockId());
    sb.append(" | ");
    sb.append(blk.getGenerationStamp());
    sb.append(" | ");
    sb.append(blk.getNumBytes());
    sb.append("}");
    sb.append(" | ");
    sb.append(storageType);
    sb.append(" | ");
    sb.append(clientName);
    sb.append(" | ");
    sb.append(Arrays.asList(targets));
    sb.append(" | ");
    sb.append(source.toString());
    sb.append(" | ");
    sb.append(stage.toString());
    sb.append(" | ");
    sb.append(pipelineSize);
    sb.append(" | ");
    sb.append(minBytesRcvd);
    sb.append(" | ");
    sb.append(maxBytesRcvd);
    sb.append(" | ");
    sb.append(latestGenerationStamp);
    sb.append(" | ");
    sb.append(cachingStrategy.toString());
    sb.append(" | ");
    sb.append(allowLazyPersist);
    

    hprof.writeLogMessage(HProf.MessageType.DATA, "writeBlock", sb.toString());
    // LOG.info("Hprof: DATA writeBlock "+sb.toString());
    // LOG.info("Hprof: DATA writeBlock ...");

    ClientOperationHeaderProto header = DataTransferProtoUtil.buildClientHeader(
        blk, clientName, blockToken);

    ChecksumProto checksumProto =
        DataTransferProtoUtil.toProto(requestedChecksum);

    OpWriteBlockProto.Builder proto = OpWriteBlockProto.newBuilder()
        .setHeader(header)
        .setStorageType(PBHelperClient.convertStorageType(storageType))
        .addAllTargets(PBHelperClient.convert(targets, 1))
        .addAllTargetStorageTypes(
            PBHelperClient.convertStorageTypes(targetStorageTypes, 1))
        .setStage(toProto(stage))
        .setPipelineSize(pipelineSize)
        .setMinBytesRcvd(minBytesRcvd)
        .setMaxBytesRcvd(maxBytesRcvd)
        .setLatestGenerationStamp(latestGenerationStamp)
        .setRequestedChecksum(checksumProto)
        .setCachingStrategy(getCachingStrategy(cachingStrategy))
        .setAllowLazyPersist(allowLazyPersist)
        .setPinning(pinning)
        .addAllTargetPinnings(PBHelperClient.convert(targetPinnings, 1));

    if (source != null) {
      proto.setSource(PBHelperClient.convertDatanodeInfo(source));
    }

    send(out, Op.WRITE_BLOCK, proto.build());
  }

  @Override
  public void transferBlock(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken,
      final String clientName,
      final DatanodeInfo[] targets,
      final StorageType[] targetStorageTypes) throws IOException {

    OpTransferBlockProto proto = OpTransferBlockProto.newBuilder()
        .setHeader(DataTransferProtoUtil.buildClientHeader(
            blk, clientName, blockToken))
        .addAllTargets(PBHelperClient.convert(targets))
        .addAllTargetStorageTypes(
            PBHelperClient.convertStorageTypes(targetStorageTypes))
        .build();

    send(out, Op.TRANSFER_BLOCK, proto);
  }

  @Override
  public void requestShortCircuitFds(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken,
      SlotId slotId, int maxVersion, boolean supportsReceiptVerification)
      throws IOException {
    OpRequestShortCircuitAccessProto.Builder builder =
        OpRequestShortCircuitAccessProto.newBuilder()
            .setHeader(DataTransferProtoUtil.buildBaseHeader(
                blk, blockToken)).setMaxVersion(maxVersion);
    if (slotId != null) {
      builder.setSlotId(PBHelperClient.convert(slotId));
    }
    builder.setSupportsReceiptVerification(supportsReceiptVerification);
    OpRequestShortCircuitAccessProto proto = builder.build();
    send(out, Op.REQUEST_SHORT_CIRCUIT_FDS, proto);
  }

  @Override
  public void releaseShortCircuitFds(SlotId slotId) throws IOException {
    ReleaseShortCircuitAccessRequestProto.Builder builder =
        ReleaseShortCircuitAccessRequestProto.newBuilder().
            setSlotId(PBHelperClient.convert(slotId));
    SpanId spanId = Tracer.getCurrentSpanId();
    if (spanId.isValid()) {
      builder.setTraceInfo(DataTransferTraceInfoProto.newBuilder().
          setTraceId(spanId.getHigh()).
          setParentId(spanId.getLow()));
    }
    ReleaseShortCircuitAccessRequestProto proto = builder.build();
    send(out, Op.RELEASE_SHORT_CIRCUIT_FDS, proto);
  }

  @Override
  public void requestShortCircuitShm(String clientName) throws IOException {
    ShortCircuitShmRequestProto.Builder builder =
        ShortCircuitShmRequestProto.newBuilder().
            setClientName(clientName);
    SpanId spanId = Tracer.getCurrentSpanId();
    if (spanId.isValid()) {
      builder.setTraceInfo(DataTransferTraceInfoProto.newBuilder().
          setTraceId(spanId.getHigh()).
          setParentId(spanId.getLow()));
    }
    ShortCircuitShmRequestProto proto = builder.build();
    send(out, Op.REQUEST_SHORT_CIRCUIT_SHM, proto);
  }

  @Override
  public void replaceBlock(final ExtendedBlock blk,
      final StorageType storageType,
      final Token<BlockTokenIdentifier> blockToken,
      final String delHint,
      final DatanodeInfo source) throws IOException {
    OpReplaceBlockProto proto = OpReplaceBlockProto.newBuilder()
        .setHeader(DataTransferProtoUtil.buildBaseHeader(blk, blockToken))
        .setStorageType(PBHelperClient.convertStorageType(storageType))
        .setDelHint(delHint)
        .setSource(PBHelperClient.convertDatanodeInfo(source))
        .build();

    send(out, Op.REPLACE_BLOCK, proto);
  }

  @Override
  public void copyBlock(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken) throws IOException {
    OpCopyBlockProto proto = OpCopyBlockProto.newBuilder()
        .setHeader(DataTransferProtoUtil.buildBaseHeader(blk, blockToken))
        .build();

    send(out, Op.COPY_BLOCK, proto);
  }

  @Override
  public void blockChecksum(final ExtendedBlock blk,
      final Token<BlockTokenIdentifier> blockToken) throws IOException {
    OpBlockChecksumProto proto = OpBlockChecksumProto.newBuilder()
        .setHeader(DataTransferProtoUtil.buildBaseHeader(blk, blockToken))
        .build();

    send(out, Op.BLOCK_CHECKSUM, proto);
  }

  static class HProf {
    public String pathHprofFile;
    public Writer hprofWriter;
    
    public enum MessageType {
        BACK, DATA, META, HPROF
    };
    
    
    /**
     * HProf class HProf is a Hadoop-based profile that profiles user-defined
     * messages.
     */
    public HProf(String classpath) {
      this.pathHprofFile = "/home/hduser/dfs/cloud-" + classpath + "-hprof.log";
      File path = new File(this.pathHprofFile);
      
      this.initHprofWriter();
      
    }

    public void initHprofWriter() {
        // LOG.info(">> HProf.initHprofWriter");
        try {
            if (this.pathHprofFile != null) {
                this.hprofWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this.pathHprofFile)));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void writeLogMessage(MessageType type, String method, String message) {
        try {
            this.hprofWriter.append(System.currentTimeMillis() + " " + type.toString() + " " + method + ": " + message + "\n");
            this.hprofWriter.flush();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 
     * There is no need to flush the stream, since close() already does that.
     */
    public void closeHprofWriter() {
        // LOG.info(">> HProf.closeHprofWriter");
        try {
            hprofWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

  }

}
