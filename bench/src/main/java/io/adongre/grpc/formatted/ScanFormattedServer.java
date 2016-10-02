package io.adongre.grpc.formatted;

import io.adongre.grpc.raw.ScanRawServiceImpl;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;

/**
 * Created by adongre on 9/29/16.
 */
public class ScanFormattedServer {
  /* The port on which the server should run */
  private int port = 50051;
  private Server server;

  private void start() throws Exception {
    server = NettyServerBuilder.forAddress(new InetSocketAddress("localhost", port))
        .addService(new io.adongre.grpc.formatted.ScanFormattedServiceImpl())
        .executor(com.google.common.util.concurrent.MoreExecutors.directExecutor())
        .channelType(NioServerSocketChannel.class)
        .build()
        .start();
    System.out.println("Server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        ScanFormattedServer.this.stop();
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
    final ScanFormattedServer server = new ScanFormattedServer();
    server.start();
    server.blockUntilShutdown();
  }


}
