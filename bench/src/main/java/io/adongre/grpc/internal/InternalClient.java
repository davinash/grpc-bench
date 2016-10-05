package io.adongre.grpc.internal;

import io.adongre.grpc.generated.ScanFormattedResponse;
import io.adongre.grpc.generated.ScanRequest;
import io.grpc.*;
import io.grpc.protobuf.lite.ProtoLiteUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by adongre on 9/29/16.
 */
public class InternalClient {

  MethodDescriptor<ScanRequest, ScanFormattedResponse> scanMethod =
      MethodDescriptor.create(MethodDescriptor.MethodType.SERVER_STREAMING,
          "ScanFormattedServer/scanFlowControlledStreaming",
          ProtoLiteUtils.marshaller(ScanRequest.getDefaultInstance()),
          ProtoLiteUtils.marshaller(ScanFormattedResponse.getDefaultInstance()));

  private void scanExecute() throws InterruptedException {
    final int NUMBER_OF_COLUMNS = Integer.getInteger("num.columns", 6);
    final long NUM_OF_ROWS = Long.getLong("num.rows", 2_000_000L);
    final int SIZE_OF_EACH_COLUMN = Integer.getInteger("column.size", 32);
    final int BATCH_SIZE = Integer.getInteger("batch.size", 100);
    ManagedChannel channel = null;

    final int CLIENT_BLOCKING = 1;
    final int CLIENT_NON_BLOCKING = 2;

    int clientMode = Integer.getInteger("client.blocking", CLIENT_BLOCKING);
    int numOfIteration = Integer.getInteger("num.iteration", 5);


    final long bytesReceived[] = {0L};
    final long rowsReceivedNum[] = {0L};
    final long startTime[] = {0L};

    try {
      channel = ManagedChannelBuilder.forAddress("localhost",
          50051)
          .usePlaintext(true)
          .executor(com.google.common.util.concurrent.MoreExecutors.directExecutor())
          .directExecutor()
          .build();

      final CountDownLatch finishLatch = new CountDownLatch(1);

      ScanRequest scanRequest = ScanRequest.newBuilder()
          .setNumOfColumns(NUMBER_OF_COLUMNS)
          .setNumOfRows(NUM_OF_ROWS)
          .setSizeOfEachColumn(SIZE_OF_EACH_COLUMN)
          .setBatchSize(BATCH_SIZE)
          .build();

      final ClientCall<ScanRequest, ScanFormattedResponse> streamingCall =
          channel.newCall(scanMethod, CallOptions.DEFAULT);

      ClientCalls.asyncServerStreamingCall(streamingCall,
          scanRequest,
          new StreamObserver<ScanFormattedResponse>() {
            @Override
            public void onNext(ScanFormattedResponse value) {

            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {
              finishLatch.countDown();
            }
          });

      finishLatch.await(30, TimeUnit.MINUTES);

    } finally {
      if (channel != null) {
        try {
          channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public static void main(String[] args) throws InterruptedException {
    new InternalClient().scanExecute();
  }
}
