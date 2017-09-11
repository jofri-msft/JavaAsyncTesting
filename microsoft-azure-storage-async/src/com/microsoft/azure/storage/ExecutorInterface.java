package com.microsoft.azure.storage;

import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import org.joda.time.DateTime;
import rx.Observable;
import rx.functions.Func1;
import rx.observables.MathObservable;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CountDownLatch;


public abstract class ExecutorInterface {
    public static final String URL = "http://xclientdev2.blob.core.windows.net";
    public static final String SAS_TOKEN = "?sv=2017-04-17&ss=bfqt&srt=sco&sp=rwdlacup&se=2017-09-02T23:58:18Z&st=2017-08-30T15:58:18Z&spr=https,http&sig=9jNzgUBRW4riOx9vBnB1Bu%2FTGfIHEY1hUU3UtDDCdiU%3D";


    private static final String FILE_ROOT = System.getenv("BENCH_FILE_ROOT") == null
            ? "C:\\Users\\jofriedm\\Desktop\\"
            : System.getenv("BENCH_FILE_ROOT");

    //public abstract Observable<Long> downloadAsync(String url, String size, int times);
    //public abstract Observable<Long> uploadAsync(String url, String blobName, String filePath, long fileSize, long blockSize);

    public Observable<Long> downloadAsync(final NettyRxAdapter client, final String url, final String size, final int times) {
        final int port;
        if (url.startsWith("https")) {
            port = 443;
        }
        else
        {
            port = 80;
        }

        return Observable.range(1, times)
                //.map(i -> new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, String.format("%s:%d/downloads/%s.dat", url, port, size)))
                .map(new Func1<Integer, DefaultFullHttpRequest>() {
                    @Override
                    public DefaultFullHttpRequest call(Integer integer) {
                        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, String.format("%s:%d/downloads/%s.dat", url, port, size));
                    }
                })
                //.flatMap(req -> client.sendRequestAsync(req))
                .flatMap(new Func1<DefaultFullHttpRequest, Observable<ByteBuf>>() {
                    @Override
                    public Observable<ByteBuf> call(DefaultFullHttpRequest request) {
                        return client.sendRequestAsync(request);
                    }
                })
//                        .map(buf -> {
//                            long bytes =  buf.readableBytes();
////                            System.out.println("Thread is " + Thread.currentThread().getName() + ", received bytes: " + bytes);
//                            buf.release();
//                            return bytes;
//                        });
                .map(new Func1<ByteBuf, Long>() {
                    @Override
                    public Long call(ByteBuf buf) {
                        long bytes = buf.readableBytes();
////                            System.out.println("Thread is " + Thread.currentThread().getName() + ", received bytes: " + bytes);
                        buf.release();
                        return bytes;
                    }
                });
        //});
    }

    public Observable<Long> uploadAsync(String url, String blobName, String filePath, long streamSize, long blockSize) {
//                BlockIdGenerator generator = new BlockIdGenerator(streamSize, blockSize/100);
        final int port;
        if (url.startsWith("https")) {
            port = 443;
        }
        else
        {
            port = 80;
        }

        final String blobUrl = String.format("%s:%d/disks/%s%s", url, port, blobName, ExecutorInterface.SAS_TOKEN);
        final String pageUrl = String.format("%s:%d/disks/%s%s&comp=page", url, port, blobName, ExecutorInterface.SAS_TOKEN);
//                final String blocklistUrl = String.format("%s:443/disks/%s%s&comp=blocklist", url, blobName, Benchmark.SAS_TOKEN);

        RandomAccessFile file;
        try {
            file = new RandomAccessFile(filePath, "r");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        final FileChannel fileChannel = file.getChannel();

        try {
            return Observable.just(blobName)
                    .flatMap(name -> {
                        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                HttpMethod.PUT,
                                blobUrl);
                        request.headers().add("Content-Length", "0");
                        request.headers().add("x-ms-version", "2017-04-17");
                        request.headers().add("x-ms-blob-type", "PageBlob");
                        request.headers().add("x-ms-blob-content-length", (long) Math.ceil(((float) streamSize)/512) * 512L);
                        request.headers().add("x-ms-date", ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.RFC_1123_DATE_TIME));
                        return client.sendRequestAsync(request);
                    })
                    .flatMap(res -> Observable.range(0, (int) Math.ceil((float) streamSize / blockSize)))
                    .map(i -> i * blockSize)
                    .map(pos -> {
                        try {
                            long count = pos + blockSize > streamSize ? (streamSize - pos) : blockSize;
                            long cap = (long) Math.ceil(((float) count)/512) * 512L;
                            ByteBuf direct = PooledByteBufAllocator.DEFAULT.buffer((int) cap, (int) cap);
                            direct.writeBytes(fileChannel, pos, (int) count);
                            direct.writeZero((int) (cap - count));
                            return Maps.immutableEntry(pos, direct);
                        } catch (IOException e) {
                            return null;
                        }
                    })
                    .flatMap(buf -> {
                        final long readable = buf.getValue().readableBytes();
                        final long cap = buf.getValue().capacity();
                        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                HttpMethod.PUT,
                                pageUrl,
                                buf.getValue());
                        request.headers().add("x-ms-range", String.format("bytes=%d-%d", buf.getKey(), buf.getKey() + cap - 1));
                        request.headers().add("Content-Length", String.valueOf(cap));
                        request.headers().add("x-ms-version", "2017-04-17");
                        request.headers().add("x-ms-page-write", "Update");
                        request.headers().add("x-ms-date", ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.RFC_1123_DATE_TIME));
                        return client.sendRequestAsync(request).map(res -> readable);
                    }).reduce((a, b) -> a + b)/*.flatMap(sum -> {
                                System.out.println("Committing...");
                                DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                                        HttpMethod.PUT,
                                        blocklistUrl,
                                        Unpooled.wrappedBuffer(generator.getBlockListXml().getBytes()));
                                request.headers().add("x-ms-date", ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.RFC_1123_DATE_TIME));
                                return client.sendRequestAsync(request).map(v -> sum).doOnCompleted(() -> System.out.println("Committed"));
                            })*/;
        } catch (RuntimeException e) {
            throw e;
        }
    }




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
