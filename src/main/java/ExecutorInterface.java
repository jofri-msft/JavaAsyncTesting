import org.joda.time.DateTime;
import rx.Observable;
import rx.observables.MathObservable;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jianghlu on 7/20/17.
 */
public abstract class ExecutorInterface {
    public static final String URL = "http://xclientdev2.blob.core.windows.net";
    public static final String SAS_TOKEN = "?sv=2017-04-17&ss=bfqt&srt=sco&sp=rwdlacup&se=2017-09-02T23:58:18Z&st=2017-08-30T15:58:18Z&spr=https,http&sig=9jNzgUBRW4riOx9vBnB1Bu%2FTGfIHEY1hUU3UtDDCdiU%3D";

    public static final String DISK_URL = "https://md-plmxncpngklx.blob.core.windows.net/2zhdpr4rmlc4/abcd?sv=2016-05-31&sr=b&si=e1fdba9d-dd94-48e6-be99-23a612f5bb51&sig=L7TSlX7UqRqy%2FMo2dnYQBZy60%2Fi7U%2FTejwqHYNmFG6E%3D";

    private static final String FILE_ROOT = System.getenv("BENCH_FILE_ROOT") == null
            ? "D:\\"
            : System.getenv("BENCH_FILE_ROOT");

    public abstract Observable<Long> downloadAsync(String url, String size, int times);
    public abstract Observable<Long> uploadAsync(String url, String blobName, String filePath, long fileSize, long blockSize);
    public Observable<Long> crossCopyAsync(String source, String dest, String blobName, String destSasToken) {
        return null;
    }

    private void prepare() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        downloadAsync(URL, "1k", 1)
                .doOnCompleted(latch::countDown)
                .subscribe();

        latch.await();
    }

    public void run() throws Exception {
//        uploadTxt();
//        uploadVhd();
//        prepare();
//        logDownloadTitle();
  //      downloadOne1KB();
//        downloadHundred1KB();
//        downloadThousand1KB();
//        downloadOne1MB();
//        downloadHundred1MB();
//        downloadThousand1MB();
//        downloadOne1GB();
//        downloadTwenty1GB();
//        logUploadTitle();
        uploadOne1KB();
//        uploadOne1MB();
        //uploadOne1GB();
//        uploadOne1GBBigChunks();
//        uploadOne10GB();
//        uploadOne10GBBigChunks();
//        uploadOne100GB();
//        uploadOne100GBBigChunks();
//        upload20One1GB();
    }

    public void crossCopy() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);
        final long fileSize = 300647711232L;
        final String sasToken = "?sv=2017-04-17&ss=b&srt=sco&sp=rwdlac&se=2017-09-08T22:00:34Z&st=2017-08-25T14:00:34Z&spr=https,http&sig=UvR%2BC6gRK23rDYHBR9QUTfc%2FGwXUIEPhkKR8c%2BhuI9g%3D";

        crossCopyAsync("https://md-33nb4vwz3dxc.blob.core.windows.net:443/mbqjmwfpjrzr/abcd?sv=2016-05-31&sr=b&si=46796d76-78ad-48fb-998b-4371dc4b3c1f&sig=GErJ%2FAiQRDcKrOKvqf7dMefCmn%2BI8X5pWHarRwaNh8s%3D",
                "https://javasdkbenchmark.blob.core.windows.net",
                "os-disk-2.vhd", sasToken)
                .reduce((a, b) -> a + b)
                .last()
                .doOnNext(c -> {
                    logTime("137G", 1, start, c);
                    latch.countDown();
                })
                .subscribe();

        latch.await();
    }

    public void uploadVhd() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);
        final long blockSize = 4096L * 1024;
        final long fileSize = 136365212160L;

        uploadAsync(URL, "VHD", "D:\\out.vhd", fileSize, blockSize)
                .reduce((a, b) -> a + b)
                .last()
                .doOnNext(c -> {
                    logTime("137G", 33292289, start, c);
                    latch.countDown();
                })
                .subscribe();

        latch.await();
    }

    public void uploadTxt() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);
        final long blockSize = 4096L * 1024;
        final long fileSize = 6488666L;

        uploadAsync(URL, "big.txt", "E:\\work\\files\\big.txt", fileSize, blockSize)
                .reduce((a, b) -> a + b)
                .last()
                .doOnNext(c -> {
                    logTime("6488666L", 2, start, c);
                    latch.countDown();
                })
                .subscribe();

        latch.await();
    }

    public void downloadOne1KB() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);

        MathObservable.sumLong(downloadAsync(URL, "1k", 1))
                .doOnNext(s -> {
                    logTime("1k", 1, start, s);
                    latch.countDown();
                }).subscribe();

        latch.await();
    }

    public void downloadHundred1KB() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);

        MathObservable.sumLong(downloadAsync(URL, "1k", 100))
                .doOnNext(s -> {
                    logTime("1k", 100, start, s);
                    latch.countDown();
                }).subscribe();

        latch.await();
    }

    public void downloadThousand1KB() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);

        MathObservable.sumLong(downloadAsync(URL, "1k", 1024))
                .doOnNext(s -> {
                    logTime("1k", 1024, start, s);
                    latch.countDown();
                }).subscribe();

        latch.await();
    }

    public void downloadOne1MB() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);

        MathObservable.sumLong(downloadAsync(URL, "1m", 1))
                .doOnNext(s -> {
                    logTime("1m", 1, start, s);
                    latch.countDown();
                }).subscribe();

        latch.await();
    }

    public void downloadOne1GB() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);

        MathObservable.sumLong(downloadAsync(URL, "1g", 1))
                .doOnNext(s -> {
                    logTime("1g", 1, start, s);
                    latch.countDown();
                }).subscribe();

        latch.await();
    }

    public void downloadTwenty1GB() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);

        MathObservable.sumLong(downloadAsync(URL, "1g", 20))
                .doOnNext(s -> {
                    logTime("1g", 20, start, s);
                    latch.countDown();
                }).subscribe();

        latch.await();
    }

    public void downloadHundred1MB() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);

        MathObservable.sumLong(downloadAsync(URL, "1m", 100))
                .doOnNext(s -> {
                    logTime("1m", 100, start, s);
                    latch.countDown();
                }).subscribe();

        latch.await();
    }

    public void downloadThousand1MB() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);

        MathObservable.sumLong(downloadAsync(URL, "1m", 1024))
                .doOnNext(s -> {
                    logTime("1m", 1024, start, s);
                    latch.countDown();
                }).subscribe();

        latch.await();
    }

    public void downOne10GB() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);

        MathObservable.sumLong(downloadAsync(URL, "10G", 1))
                .doOnNext(s -> {
                    logTime("1m", 1024, start, s);
                    latch.countDown();
                }).subscribe();

        latch.await();
    }

    public void uploadOne1KB() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);
        final long fileSize = 1024L;

        uploadAsync(URL, "some1kblob", FILE_ROOT + "1k.dat", fileSize, 4096L * 1024)
                .reduce((a, b) -> a + b)
                .last()
                .doOnNext(c -> {
                    logTime("1k", 1, start, c);
                    latch.countDown();
                })
                .subscribe();

        latch.await();
    }

    public void uploadOne1MB() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);
        final long fileSize = 1024L * 1024;

        uploadAsync(URL, "some1mblob", FILE_ROOT + "1m.dat", fileSize, 4096L * 1024)
                .reduce((a, b) -> a + b)
                .last()
                .doOnNext(c -> {
                    logTime("1m", 1, start, c);
                    latch.countDown();
                })
                .subscribe();

        latch.await();
    }

    public void uploadOne1GB() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);
        final long blockSize = 4096L * 1024;
        final long fileSize = 1024L * 1024 * 1024;

        uploadAsync(URL, "some1gblob", FILE_ROOT + "1g.dat", fileSize, blockSize)
                .reduce((a, b) -> a + b)
                .last()
                .doOnNext(c -> {
                    logTime("1g", 256, start, c);
                    latch.countDown();
                })
                .subscribe();

        latch.await();
    }

    public void uploadOne1GBBigChunks() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);
        final long blockSize = 100L * 1024 * 1024;
        final long fileSize = 1024L * 1024 * 1024;

        uploadAsync(URL, "some1gblob", FILE_ROOT + "1g.dat", fileSize, blockSize)
                .reduce((a, b) -> a + b)
                .last()
                .doOnNext(c -> {
                    logTime("1g", (int) (fileSize / blockSize), start, c);
                    latch.countDown();
                })
                .subscribe();

        latch.await();
    }

    public void uploadOne10GB() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);
        final long blockSize = 4096L * 1024;
        final long fileSize = 10240L * 1024 * 1024;

        uploadAsync(URL, "some10gblob", FILE_ROOT + "10g.dat", fileSize, blockSize)
                .reduce((a, b) -> a + b)
                .last()
                .doOnNext(c -> {
                    logTime("10g", 2560, start, c);
                    latch.countDown();
                })
                .subscribe();

        latch.await();
    }

    public void uploadOne10GBBigChunks() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);
        final long blockSize = 100L * 1024 * 1024;
        final long fileSize = 10240L * 1024 * 1024;

        uploadAsync(URL, "some10gblob", FILE_ROOT + "10g.dat", fileSize, blockSize)
                .reduce((a, b) -> a + b)
                .last()
                .doOnNext(c -> {
                    logTime("10g", (int) (fileSize / blockSize), start, c);
                    latch.countDown();
                })
                .subscribe();

        latch.await();
    }

    public void upload20One1GB() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);
        final long blockSize = 128L * 1024 * 1024;
        final long fileSize = 1024L * 1024 * 1024;

        Observable.range(1, 20)
                .flatMap(i -> {
                    return uploadAsync(URL, "some1gblob" + i, FILE_ROOT + "1g.dat", fileSize, blockSize)
                            .reduce((a, b) -> a + b)
                            .last();
                })
                .last()
                .doOnNext(c -> {
                    logTime("20x1g", (int) (fileSize / blockSize) * 20, start, c);
                    latch.countDown();
                })
                .subscribe();

        latch.await();
    }

    private void logDownloadTitle() {
        System.out.println("Size\tTimes\tSeconds\t\tDownloaded\t\tSpeed(MB/s)");
        System.out.println("--------------------------------------------------------");
    }

    private void logUploadTitle() {
        System.out.println("Size\tChunks\tSeconds\t\tUploaded\t\tSpeed(MB/s)");
        System.out.println("--------------------------------------------------------");
    }

    private void logTime(String size, int chunks, DateTime start, long downloaded) {
        float seconds = (DateTime.now().getMillis() - start.getMillis()) / (float) 1000;
        System.out.println(size + "\t\t" + chunks + "\t\t" + seconds +
                "\t\t" + downloaded + (downloaded >= 10000000L ? "\t\t" : "\t\t\t") +
                (downloaded / seconds / 1024 / 1024));
    }

    private long exhaustStream(InputStream inputStream) throws IOException {
        long i = inputStream.skip(1000);
        long count = i;
        while (i > 0) {
            i = inputStream.skip(1000);
            count += i;
        }
        inputStream.close();
        return count;
    }
}
