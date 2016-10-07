package io.adongre.grpc.internal;

import com.google.protobuf.ByteString;
import io.adongre.grpc.generated.ColumnValues;
import io.adongre.grpc.generated.ScanFormattedResponse;
import io.adongre.grpc.generated.ScanFormattedRow;
import io.adongre.grpc.generated.ScanRequest;
import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.lite.ProtoLiteUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by adongre on 9/29/16.
 */
public class InternalServer {
  /* The port on which the server should run */
  private int port = 50051;
  private Server server;

  private void start() throws Exception {
    NettyServerBuilder serverBuilder = NettyServerBuilder.forAddress(new InetSocketAddress("localhost", port))
        .executor(com.google.common.util.concurrent.MoreExecutors.directExecutor())
        .channelType(NioServerSocketChannel.class);


    serverBuilder.addService(
        ServerServiceDefinition.builder(
            new ServiceDescriptor("AmpoolCore",
                MethodDeclarations.scanMethod))
            .addMethod(MethodDeclarations.scanMethod, new ServerCallHandler<ScanRequest, ScanFormattedResponse>() {
              @Override
              public ServerCall.Listener<ScanRequest> startCall(
                  final ServerCall<ScanRequest, ScanFormattedResponse> call,
                  Metadata headers) {
                call.sendHeaders(new Metadata());
                call.request(1);
                return new ServerCall.Listener<ScanRequest>() {
                  final BlockingQueue<ScanFormattedResponse> Q =
                      new ArrayBlockingQueue<ScanFormattedResponse>(1000);

                  @Override
                  public void onMessage(ScanRequest request) {

                    System.out.println("Request recieved for a scan");
                    MapReaderThread MRT = new MapReaderThread(request, this.Q);
                    Thread mapReaderThread = new Thread(MRT, "MapReaderThread");
                    mapReaderThread.start();
                  }

                  @Override
                  public void onHalfClose() {
                    call.close(Status.OK, new Metadata());
                  }

                  @Override
                  public void onCancel() {

                  }

                  @Override
                  public void onComplete() {

                  }

                  @Override
                  public void onReady() {
                    try {
                      this.Q.take();
                    } catch (InterruptedException e) {
                      e.printStackTrace();
                    }

                    while (call.isReady()) {
                      //try {
                        call.sendMessage(ScanFormattedResponse.newBuilder().build());
                        System.out.println("Sent a Message");
                      //} catch (InterruptedException e) {
//                        e.printStackTrace();
  //                    }
                    }
                  }
                };
              }
            }).build());


//
//
//
//    serverBuilder.addService(
//        ServerServiceDefinition.builder("ScanFormattedServer")
//            .addMethod(scanMethod, new ServerCallHandler<ScanRequest, ScanFormattedResponse>() {
//              @Override
//              public ServerCall.Listener<ScanRequest> startCall(
//                  final ServerCall<ScanRequest, ScanFormattedResponse> call,
//                  Metadata headers) {
//                call.sendHeaders(new Metadata());
//                call.request(1);
//
//                return new ServerCall.Listener<ScanRequest>() {
//                  int numOfColumns = 0;
//                  int columnSize = 0;
//                  int batchSize = 0;
//                  byte[] data = null;
//                  long numOfRows = 0L;
//                  long counter = 0L;
//
//                  ScanFormattedResponse.Builder scanFormattedResponseBuilder = ScanFormattedResponse.newBuilder();
//                  ScanFormattedRow.Builder scanFormattedRowBuilder = ScanFormattedRow.newBuilder();
//                  ColumnValues.Builder columnValueBuilder = ColumnValues.newBuilder();
//
//
//                  @Override
//                  public void onMessage(ScanRequest request) {
//
//                    System.out.println("InternalServer.onMessage");
//
//                    this.numOfColumns = request.getNumOfColumns();
//                    this.columnSize = request.getSizeOfEachColumn();
//                    this.batchSize = request.getBatchSize();
//                    this.data = new byte[columnSize];
//                    this.numOfRows = request.getNumOfRows();
//
////                    System.out.println("numOfColumns = " + numOfColumns);
////                    System.out.println("columnSize   = " + columnSize);
////                    System.out.println("batchSize    = " + batchSize);
////                    System.out.println("numOfRows    = " + numOfRows);
////
////                    while (call.isReady()) {
////
////                    for (long i = 0; i < this.numOfRows; i++) {
////                      for (int columnIdx = 0; columnIdx < numOfColumns; columnIdx++) {
////                        this.columnValueBuilder.setColumnName(
////                            ByteString.copyFrom(ByteBuffer.allocate(Long.BYTES).putLong(i).array()));
////                        this.columnValueBuilder.setColumnValue(
////                            ByteString.copyFrom(data));
////                        this.scanFormattedRowBuilder.addColumnValue(this.columnValueBuilder.build());
////                        this.columnValueBuilder.clear();
////                      } // numOfColumns ends
////                      this.scanFormattedRowBuilder.setRowId(i);
////                      this.scanFormattedRowBuilder.setTimeStamp(System.nanoTime());
////
////                      this.scanFormattedResponseBuilder.addRow(this.scanFormattedRowBuilder.build());
////                      this.scanFormattedRowBuilder.clear();
//////                      while ( call.isReady()) {
//////                        System.out.println("InternalServer.onReady  -> " + counter++);
//////                        call.sendMessage(this.scanFormattedResponseBuilder.build());
//////                        this.scanFormattedResponseBuilder.clear();
//////                      }
//                      onReady();
////
////
////                        call.sendMessage(this.scanFormattedResponseBuilder.build());
////                      }
////                    } // this.numOfRows loop ends
//                  }
//
//                  @Override
//                  public void onHalfClose() {
//                    call.close(Status.OK, new Metadata());
//                  }
//
//                  @Override
//                  public void onCancel() {
//                    System.out.println("ScanFormattedServer.onCancel");
//                  }
//
//                  @Override
//                  public void onComplete() {
//                    System.out.println("ScanFormattedServer.onComplete");
//                  }
//
//                  @Override
//                  public void onReady() {
////                    while ( call.isReady()) {
////                      //System.out.println("InternalServer.onReady  -> " + counter++);
////                      call.sendMessage(this.scanFormattedResponseBuilder.build());
////                      this.scanFormattedResponseBuilder.clear();
////                    }
//
//                    System.out.println("numOfColumns = " + numOfColumns);
//                    System.out.println("columnSize   = " + columnSize);
//                    System.out.println("batchSize    = " + batchSize);
//                    System.out.println("numOfRows    = " + numOfRows);
//
//                    while (call.isReady()) {
//
//                      for (long i = 0; i < this.numOfRows; i++) {
//                        for (int columnIdx = 0; columnIdx < numOfColumns; columnIdx++) {
//                          this.columnValueBuilder.setColumnName(
//                              ByteString.copyFrom(ByteBuffer.allocate(Long.BYTES).putLong(i).array()));
//                          this.columnValueBuilder.setColumnValue(
//                              ByteString.copyFrom(data));
//                          this.scanFormattedRowBuilder.addColumnValue(this.columnValueBuilder.build());
//                          this.columnValueBuilder.clear();
//                        } // numOfColumns ends
//                        this.scanFormattedRowBuilder.setRowId(i);
//                        this.scanFormattedRowBuilder.setTimeStamp(System.nanoTime());
//
//                        this.scanFormattedResponseBuilder.addRow(this.scanFormattedRowBuilder.build());
//                        this.scanFormattedRowBuilder.clear();
////                      while ( call.isReady()) {
////                        System.out.println("InternalServer.onReady  -> " + counter++);
////                        call.sendMessage(this.scanFormattedResponseBuilder.build());
////                        this.scanFormattedResponseBuilder.clear();
////                      }
//                        //onReady();
//
//
//                        call.sendMessage(this.scanFormattedResponseBuilder.build());
//                      }
//                    }
//                  }
//                };
//              }
//            }).build()
//    );

    this.server = serverBuilder.build();
    this.server.start();

    System.out.println("BenchmarkServer started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        InternalServer.this.stop();
        System.err.println("*** server shut down");
      }
    });
  }

  private void stop() {
    if (server != null) {
      server.shutdown();
    }
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }


  public static void main(String[] args) throws Exception {
    final InternalServer server = new InternalServer();
    server.start();
    server.blockUntilShutdown();
  }


}
