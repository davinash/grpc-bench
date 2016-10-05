package io.adongre.grpc.internal;

import io.adongre.grpc.generated.ScanFormattedResponse;
import io.adongre.grpc.generated.ScanRequest;
import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.lite.ProtoLiteUtils;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;

/**
 * Created by adongre on 9/29/16.
 */
public class InternalServer {
  /* The port on which the server should run */
  private int port = 50051;
  private Server server;

  MethodDescriptor<ScanRequest, ScanFormattedResponse> scanMethod =
      MethodDescriptor.create(MethodDescriptor.MethodType.SERVER_STREAMING,
          "ScanFormattedServer/scanFlowControlledStreaming",
          ProtoLiteUtils.marshaller(ScanRequest.getDefaultInstance()),
          ProtoLiteUtils.marshaller(ScanFormattedResponse.getDefaultInstance()));

  private void start() throws Exception {
    NettyServerBuilder serverBuilder = NettyServerBuilder.forAddress(new InetSocketAddress("localhost", port))
        //.addService(new io.adongre.grpc.formatted.ScanFormattedServiceImpl())
        .executor(com.google.common.util.concurrent.MoreExecutors.directExecutor())
        .channelType(NioServerSocketChannel.class);



    ScanFormattedResponse.Builder scanResponseBuilder = ScanFormattedResponse.newBuilder();
    serverBuilder.addService(
        ServerServiceDefinition.builder("ScanFormattedServer")
            .addMethod(scanMethod, new ServerCallHandler<ScanRequest, ScanFormattedResponse>() {
              @Override
              public ServerCall.Listener<ScanRequest> startCall(
                  final ServerCall<ScanRequest, ScanFormattedResponse> call,
                  Metadata headers) {
                call.sendHeaders(new Metadata());
                call.request(1);
                return new ServerCall.Listener<ScanRequest>() {
                  @Override
                  public void onMessage(ScanRequest message) {
                    // no-op
                    System.out.println("ScanFormattedServer.onMessage");
                    call.sendMessage(scanResponseBuilder.build());
                  }

                  @Override
                  public void onHalfClose() {
                    System.out.println("ScanFormattedServer.onHalfClose");
                    call.close(Status.OK, new Metadata());
                  }

                  @Override
                  public void onCancel() {
                    System.out.println("ScanFormattedServer.onCancel");
                  }

                  @Override
                  public void onComplete() {
                    System.out.println("ScanFormattedServer.onComplete");
                  }

                  @Override
                  public void onReady() {
                    System.out.println("ScanFormattedServer.onReady");
                    super.onReady();
                  }
                };
              }
            }).build()
    );

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
