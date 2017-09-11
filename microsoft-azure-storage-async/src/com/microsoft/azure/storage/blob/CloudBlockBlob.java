package com.microsoft.azure.storage.async.blob;


import com.google.common.collect.Maps;
import com.microsoft.azure.storage.blob.CloudBlob;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import org.joda.time.DateTime;
import rx.Observable;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CountDownLatch;

public class CloudBlockBlob extends CloudBlob {



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
}
