package io.adongre.grpc.internal;

import io.adongre.grpc.generated.ColumnValues;
import io.adongre.grpc.generated.ScanFormattedResponse;
import io.adongre.grpc.generated.ScanFormattedRow;
import io.adongre.grpc.generated.ScanRequest;
import io.grpc.*;
import io.grpc.protobuf.lite.ProtoLiteUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by adongre on 9/29/16.
 */
public class InternalClient {

  private void clientRequestExecute() throws InterruptedException {
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

      final AtomicReference<StreamObserver<ScanRequest>> requestObserverRef =
          new AtomicReference<StreamObserver<ScanRequest>>();

      final ClientCall<ScanRequest, ScanFormattedResponse> streamingCall =
          channel.newCall(MethodDeclarations.scanMethod, CallOptions.DEFAULT);

      ClientCalls.asyncServerStreamingCall(streamingCall,
          scanRequest,
          new StreamObserver<ScanFormattedResponse>() {
            @Override
            public void onNext(ScanFormattedResponse scanFormattedResponse) {
              System.out.println("InternalClient.onNext");
              bytesReceived[0] += 8; // timestamp
              bytesReceived[0] += 8; // row id
              Iterator<ScanFormattedRow> scanFormattedRowIterator = scanFormattedResponse.getRowList().iterator();
              while (scanFormattedRowIterator.hasNext()) {
                System.out.println("InternalClient.onNext.Reading Rows");
                ScanFormattedRow scanFormattedRow = scanFormattedRowIterator.next();
                Iterator<ColumnValues> columnValuesIterator = scanFormattedRow.getColumnValueList().iterator();
                while (columnValuesIterator.hasNext()) {
                  ColumnValues columnValues = columnValuesIterator.next();
                  bytesReceived[0] += columnValues.getColumnName().toByteArray().length;
                  bytesReceived[0] += columnValues.getColumnValue().toByteArray().length;
                }
                rowsReceivedNum[0]++;
              }
            }

            @Override
            public void onError(Throwable t) {
              finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
              double seconds = (System.nanoTime() - startTime[0]) / 1000000000.0;
              System.out.println();
              System.out.println("Num Of Rows     -> " + rowsReceivedNum[0]);
              System.out.println("Time            -> " + seconds + " Seconds");
              System.out.println("Total Data Size -> " + bytesReceived[0]);
              System.out.println("Data Rate       -> " + ((bytesReceived[0] / 1024L / 1024L) / seconds) + " MBps");
              System.out.println();
              bytesReceived[0] = 0L;
              rowsReceivedNum[0] = 0L;
              finishLatch.countDown();
            }
          });
/*
      requestObserverRef.set(requestObserver);
      requestObserver.onNext(request.slice());
*/

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
    new InternalClient().clientRequestExecute();
  }
}
