import org.joda.time.DateTime;

import java.util.concurrent.CountDownLatch;publlic class CloudPageBlob extends CloudBlob {

//    public void uploadFromFile(String fileName) throws Exception {
//        final DateTime start = DateTime.now();
//        final CountDownLatch latch = new CountDownLatch(1);
//        final long blockSize = 4096L * 1024;
//        final long fileSize = 136365212160L;
//
//        uploadAsync(URL, "VHD", "D:\\out.vhd", fileSize, blockSize)
//                .reduce((a, b) -> a + b)
//                .last()
//                .doOnNext(c -> {
//                    logTime("137G", 33292289, start, c);
//                    latch.countDown();
//                })
//                .subscribe();
//
//        latch.await();
//    }
}