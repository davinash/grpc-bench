package io.adongre.grpc.raw;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * Created by adongre on 9/29/16.
 */
public class ScanRawClient {
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
      Iterator<io.adongre.grpc.generated.ScanRawResponse> scanResultIterator =
          blockingStub.rawScan(io.adongre.grpc.generated.ScanRequest.newBuilder()
              .setNumOfColumns(NUMBER_OF_COLUMNS)
              .setNumOfRows(NUM_OF_ROWS)
              .setSizeOfEachColumn(SIZE_OF_EACH_COLUMN)
              .build());

      while (scanResultIterator.hasNext()) {
        io.adongre.grpc.generated.ScanRawResponse scanResult = scanResultIterator.next();
        Iterator<io.adongre.grpc.generated.ScanRawRow> scanRowIterator = scanResult.getRowList().iterator();
        while (scanRowIterator.hasNext()) {
          io.adongre.grpc.generated.ScanRawRow scanRow = scanRowIterator.next();
          bytesReceived += scanRow.getRow().toByteArray().length;
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
    new ScanRawClient().scanExecute();
  }
}
