import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import org.junit.BeforeClass;
import org.junit.Test;
import rx.Observable;
import rx.exceptions.Exceptions;
import rx.functions.Func1;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.channels.FileChannel;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by jianghlu on 7/20/17.
 */
public class NettyTests {

    private static NettyRxAdapter client;
    private static ExecutorInterface executionInterface;

    @BeforeClass
    public static void setup() throws Exception {
        client = new NettyRxAdapter();

        executionInterface = new ExecutorInterface() {
            @Override
            public Observable<Long> downloadAsync(final String url, final String size, final int times) {
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

            @Override
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

            @Override
            public Observable<Long> crossCopyAsync(String source, String dest, String blobName, String destSasToken) {
                BlockIdGenerator generator = new BlockIdGenerator(136365212160L, 8192 * 10);
                int port;
                if (dest.startsWith("https")) {
                    port = 443;
                }
                else
                {
                    port = 80;
                }

                final String blockUrl = String.format("%s:80/disks/%s%s&comp=block", dest, blobName, destSasToken);
                final String blocklistUrl = String.format("%s:80/disks/%s%s&comp=blocklist", dest, blobName, destSasToken);

                File file = new File("D:\\better.vhd");
                FileChannel fileChannel;
                try {
                    file.createNewFile();
                    fileChannel = new RandomAccessFile(file, "rw").getChannel();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                final long streamSize = 136365212160L;
                final long blockSize = 16384 * 1024;
                return Observable.range(0, (int) Math.ceil((float) 136365212160L / blockSize))
                        .map(i -> i * blockSize)
                        .flatMap(pos -> {
                            long end = pos + blockSize;
                            if (end >= streamSize) {
                                end = streamSize - 1;
                            }
                            return Observable.just(String.format("bytes=%d-%d", pos, end))
                                    .map(header -> {
                                        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, source);
                                        request.headers().add("x-ms-range", header);
                                        return request;
                                    })
                                    .concatMap(req -> client.sendRequestAsync(req))
                                    .retry(5)
                                    .concatMap(buf -> {
                                        long readable = buf.readableBytes();
                                        try {
                                            fileChannel.write(buf.nioBuffer(), pos);
                                            buf.release();
                                        } catch (IOException e) {
                                            throw new RuntimeException(e);
                                        }
                                        return Observable.just(readable);
                                    });
                        }).reduce((a, b) -> a + b).doOnCompleted(() -> {
                            try {
                                fileChannel.close();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
        };
    }

    private synchronized static long usedDirectoryMemory() {
        long total = 0;
        for (PoolArenaMetric metric : PooledByteBufAllocator.DEFAULT.directArenas()) {
            total += metric.numActiveBytes();
        }
        return total;
    }

    @Test
    public void run() throws Exception {
        System.out.println("--------NETTY---------");
        for (int i = 0 ; i < 1; i++) {
            executionInterface.run();
        }
    }
}
