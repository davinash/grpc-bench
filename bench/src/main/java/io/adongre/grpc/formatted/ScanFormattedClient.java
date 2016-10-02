package io.adongre.grpc.formatted;

import io.adongre.grpc.generated.ColumnValues;
import io.adongre.grpc.generated.ScanFormattedResponse;
import io.adongre.grpc.generated.ScanFormattedRow;
import io.adongre.grpc.generated.ScanRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * Created by adongre on 9/29/16.
 */
public class ScanFormattedClient {
  private void scanExecute() throws InterruptedException {
    final int NUMBER_OF_COLUMNS = 1024;
    final long NUM_OF_ROWS = 2_000_000L;
    final int SIZE_OF_EACH_COLUMN = 32;
    ManagedChannel channel = null;

    try {
      channel = ManagedChannelBuilder.forAddress("localhost",
          50051)
          .usePlaintext(true)
          .executor(com.google.common.util.concurrent.MoreExecutors.directExecutor())
          .build();
      io.adongre.grpc.generated.ScanServiceGrpc.ScanServiceBlockingStub blockingStub =
          io.adongre.grpc.generated.ScanServiceGrpc.newBlockingStub(channel);

      long startTime = System.nanoTime();
      long bytesReceived = 0L;
      long rowsReceivedNum = 0L;
      Iterator<ScanFormattedResponse> scanResultIterator =
          blockingStub.formatedScan(ScanRequest.newBuilder()
              .setNumOfColumns(NUMBER_OF_COLUMNS)
              .setNumOfRows(NUM_OF_ROWS)
              .setSizeOfEachColumn(SIZE_OF_EACH_COLUMN)
              .build());

      while (scanResultIterator.hasNext()) {
        ScanFormattedResponse scanFormattedResponse = scanResultIterator.next();
        Iterator<ScanFormattedRow> scanFormattedRowIterator = scanFormattedResponse.getRowList().iterator();
        while (scanFormattedRowIterator.hasNext()) {
          ScanFormattedRow scanFormattedRow = scanFormattedRowIterator.next();
          bytesReceived += 8; // timestamp
          bytesReceived += 8; // row id
          Iterator<ColumnValues> columnValuesIterator = scanFormattedRow.getColumnValueList().iterator();
          while (columnValuesIterator.hasNext()) {
            ColumnValues columnValues = columnValuesIterator.next();
            bytesReceived += columnValues.getColumnName().toByteArray().length;
            bytesReceived += columnValues.getColumnValue().toByteArray().length;
          }
          rowsReceivedNum++;
        }
      }

      double seconds = (System.nanoTime() - startTime) / 1000000000.0;
      System.out.println();
      System.out.println("Num Of Rows     -> " + rowsReceivedNum);
      System.out.println("Time            -> " + seconds + " Seconds");
      System.out.println("Total Data Size -> " + bytesReceived);
      System.out.println("Data Rate       -> " + ((bytesReceived / 1024L / 1024L) / seconds) + " MBps");
      System.out.println();
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
