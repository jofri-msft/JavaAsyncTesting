import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import org.joda.time.DateTime;
import rx.Observable;
import rx.exceptions.Exceptions;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.channels.FileChannel;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Created by jianghlu on 8/23/2017.
 *
 * This sample uploads a 1g file many times.
 */
public class SimpleUpload {
    private static final long BLOCK_SIZE = 100L * 1024 * 1024;
    private static final String URL = "https://sdkbenchmark.blob.core.windows.net:443";

    public static void main(String args[]) {
//        NettyDummyAdapter adapter = new NettyDummyAdapter();
//        uploadRx(adapter);
//        uploadFuture(adapter);
    }

    private static Map<String, String> readFiles() {
        return new HashMap<String, String>() {{
            put("simple1g-a.dat", "E:\\work\\files\\10g.dat");
            put("simple1g-b.dat", "E:\\work\\files\\10g.dat");
            put("simple1g-c.dat", "E:\\work\\files\\10g.dat");
            put("simple1g-d.dat", "E:\\work\\files\\10g.dat");
        }};
    }

    public static void uploadRx(final NettyDummyAdapter adapter) {
        final DateTime start = DateTime.now();

        final Function<Map.Entry<String, FileChannel>, Observable<Long>> upload = (fc) -> {
            try {
                final long fileSize = fc.getValue().size();
                BlockIdGenerator generator = new BlockIdGenerator(fileSize, BLOCK_SIZE);
                final String blockUrl = String.format("%s/disks/%s%s&comp=block", URL, fc.getKey(), ExecutorInterface.SAS_TOKEN);
                final String blockListUrl = String.format("%s/disks/%s%s&comp=blocklist", URL, fc.getKey(), ExecutorInterface.SAS_TOKEN);

                // Divide into chunks
                return Observable.range(0, (int) Math.ceil((float) fileSize / BLOCK_SIZE))
                        // Map to the beginning position of the chunk
                        .map(i -> i * BLOCK_SIZE)
                        // Read into direct buffer
                        .map(pos -> allocByteBuf(pos, fileSize, fc.getValue()))
                        // Send the block
                        .concatMap(buf -> {
                            long size = buf.readableBytes();
                            return adapter.sendRequestAsync(createBlockRequest(buf, blockUrl, generator.getBlockId())).map(res -> size);
                        })
                        // Sum the bytes uploaded
                        .reduce((a, b) -> a + b)
                        // Commit the blocks
                        .flatMap(sum -> adapter.sendRequestAsync(createCommitRequest(blockListUrl, generator.getBlockListXml())).map(v -> sum));
            } catch (IOException e) {
                throw Exceptions.propagate(e);
            }
        };

        Observable<Long> uploads = Observable.from(readFiles().entrySet())
                // Read all files
                .map(f -> {
                    RandomAccessFile file;
                    try {
                        file = new RandomAccessFile(f.getValue(), "r");
                        return Maps.immutableEntry(f.getKey(), file.getChannel());
                    } catch (IOException e) {
                        throw Exceptions.propagate(e);
                    }
                })
                // For each file, upload
                .flatMap(upload::apply, (fc, sum) -> {
                    System.out.println(String.format("Uploaded %d bytes of blob %s", sum, fc.getKey()));
                    return sum;
                })
                .reduce((a, b) -> a + b)
                .doOnNext(s -> {
                    int time = (int) Duration.ofMillis(DateTime.now().getMillis() - start.getMillis()).getSeconds();
                    System.out.println("Total bytes: " + s + " Total time in seconds " + time);
                });

        uploads.toBlocking().subscribe();
    }

    public static void uploadFuture(final NettyDummyAdapter adapter) {
        final DateTime start = DateTime.now();

        final Function<Map.Entry<String, FileChannel>, ListenableFuture<Long>> upload = (fc) -> {
            try {
                final long fileSize = fc.getValue().size();
                BlockIdGenerator generator = new BlockIdGenerator(fileSize, BLOCK_SIZE);
                final String blockUrl = String.format("%s/disks/%s%s&comp=block", URL, fc.getKey(), ExecutorInterface.SAS_TOKEN);
                final String blockListUrl = String.format("%s/disks/%s%s&comp=blocklist", URL, fc.getKey(), ExecutorInterface.SAS_TOKEN);

                int chunks = (int) Math.ceil((float) fileSize / BLOCK_SIZE);
                final AtomicLong count = new AtomicLong(0);
                List<ListenableFuture<?>> futures = new ArrayList<>();

                // Divide into chunks
                for (int i = 0; i != chunks; i++) {
                    long pos = i * BLOCK_SIZE;
                    // Read into direct buffer
                    ByteBuf buf = allocByteBuf(pos, fileSize, fc.getValue());
                    count.addAndGet(buf.readableBytes());
                    // Send the block
                    futures.add(adapter.sendRequestFuture(createBlockRequest(buf, blockUrl, generator.getBlockId())));
                }

                // Commit the blocks when uploads are completed
                return Futures.whenAllComplete(futures).call(() -> {
                    adapter.sendRequestFuture(createCommitRequest(blockListUrl, generator.getBlockListXml()));
                    return count.get();
                });
            } catch (IOException e) {
                throw Exceptions.propagate(e);
            }
        };

        // Read all files
        Map<String, FileChannel> channels = Maps.transformEntries(readFiles(), (blob, filePath) -> {
            RandomAccessFile file;
            try {
                file = new RandomAccessFile(filePath, "r");
                return file.getChannel();
            } catch (IOException e) {
                throw Exceptions.propagate(e);
            }

        });

        long bytes = 0;
        try {
            // For each file, upload
            for (Map.Entry<String, FileChannel> fc : channels.entrySet()) {
                bytes += upload.apply(fc).get();
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            System.exit(1);
        }
        int time = (int) Duration.ofMillis(DateTime.now().getMillis() - start.getMillis()).getSeconds();
        System.out.println("Total bytes: " + bytes + " Total time in seconds " + time);
    }

    private static ByteBuf allocByteBuf(long pos, long fileSize, FileChannel channel) {
        long count = pos + BLOCK_SIZE > fileSize ? (fileSize - pos) : BLOCK_SIZE;
        ByteBuf direct = PooledByteBufAllocator.DEFAULT.heapBuffer((int) count, (int) count);
        try {
            direct.writeBytes(channel, pos, (int) count);
        } catch (IOException e) {
            throw Exceptions.propagate(e);
        }
        return direct;
    }

    private static DefaultFullHttpRequest createBlockRequest(ByteBuf data, String blockUrl, String blockId) {
        DefaultFullHttpRequest request = null;
        try {
            request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                    HttpMethod.PUT,
                    blockUrl + "&blockid=" + URLEncoder.encode(blockId, "UTF-8"),
                    data);
            request.headers().add("Content-Length", String.valueOf(data.capacity()));
            request.headers().add("x-ms-date", ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.RFC_1123_DATE_TIME));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return request;
    }

    private static DefaultFullHttpRequest createCommitRequest(String blockListUrl, String blockListXml) {
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
                HttpMethod.PUT,
                blockListUrl,
                Unpooled.wrappedBuffer(blockListXml.getBytes()));
        request.headers().add("x-ms-date", ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.RFC_1123_DATE_TIME));
        return request;
    }
}
