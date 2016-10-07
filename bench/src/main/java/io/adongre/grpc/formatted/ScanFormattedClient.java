package io.adongre.grpc.formatted;

import io.adongre.grpc.generated.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by adongre on 9/29/16.
 */
public class ScanFormattedClient {
  private void scanExecute() throws InterruptedException {
    final int NUMBER_OF_COLUMNS = 6;
    final long NUM_OF_ROWS = Long.getLong("num.rows",2_000_000L);
    final int SIZE_OF_EACH_COLUMN = 32;
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

      if (clientMode == CLIENT_NON_BLOCKING) {
        System.out.println(" Using  CLIENT_NON_BLOCKING ... ");
        final ScanServiceGrpc.ScanServiceStub asyncStub = ScanServiceGrpc.newStub(channel);
        for ( int i = 0; i < numOfIteration; i++) {
          startTime[0] = System.nanoTime();
          final CountDownLatch finishLatch = new CountDownLatch(1);
          asyncStub.formatedScan(ScanRequest.newBuilder()
              .setNumOfColumns(NUMBER_OF_COLUMNS)
              .setNumOfRows(NUM_OF_ROWS)
              .setBatchSize(BATCH_SIZE)
              .setSizeOfEachColumn(SIZE_OF_EACH_COLUMN)
              .build(), new StreamObserver<ScanFormattedResponse>() {
            @Override
            public void onNext(ScanFormattedResponse scanFormattedResponse) {
              bytesReceived[0] += 8; // timestamp
              bytesReceived[0] += 8; // row id
              Iterator<ScanFormattedRow> scanFormattedRowIterator = scanFormattedResponse.getRowList().iterator();
              while (scanFormattedRowIterator.hasNext()) {
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
            public void onError(Throwable throwable) {
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
          finishLatch.await(30, TimeUnit.MINUTES);
        }
      } else {
        System.out.println(" Using  CLIENT_BLOCKING ... ");
        io.adongre.grpc.generated.ScanServiceGrpc.ScanServiceBlockingStub blockingStub =
            io.adongre.grpc.generated.ScanServiceGrpc.newBlockingStub(channel);

        for (int i = 0; i < numOfIteration; i++) {
          startTime[0] = System.nanoTime();
          Iterator<ScanFormattedResponse> scanResultIterator =
              blockingStub.formatedScan(ScanRequest.newBuilder()
                  .setNumOfColumns(NUMBER_OF_COLUMNS)
                  .setNumOfRows(NUM_OF_ROWS)
                  .setBatchSize(BATCH_SIZE)
                  .setSizeOfEachColumn(SIZE_OF_EACH_COLUMN)
                  .build());

          while (scanResultIterator.hasNext()) {
            ScanFormattedResponse scanFormattedResponse = scanResultIterator.next();
            Iterator<ScanFormattedRow> scanFormattedRowIterator = scanFormattedResponse.getRowList().iterator();
            while (scanFormattedRowIterator.hasNext()) {
              ScanFormattedRow scanFormattedRow = scanFormattedRowIterator.next();
              bytesReceived[0] += 8; // timestamp
              bytesReceived[0] += 8; // row id
              Iterator<ColumnValues> columnValuesIterator = scanFormattedRow.getColumnValueList().iterator();
              while (columnValuesIterator.hasNext()) {
                ColumnValues columnValues = columnValuesIterator.next();
                bytesReceived[0] += columnValues.getColumnName().toByteArray().length;
                bytesReceived[0] += columnValues.getColumnValue().toByteArray().length;
              }
              rowsReceivedNum[0]++;
            }
          }
          double seconds = (System.nanoTime() - startTime[0]) / 1000000000.0;
          System.out.println();
          System.out.println("Num Of Rows     -> " + rowsReceivedNum[0]);
          System.out.println("Time            -> " + seconds + " Seconds");
          System.out.println("Total Data Size -> " + bytesReceived[0]);
          System.out.println("Data Rate       -> " + ((bytesReceived[0] / 1024L / 1024L) / seconds) + " MBps");
          System.out.println();
        }
      }
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
    new ScanFormattedClient().scanExecute();
  }
}
