import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.channels.FileChannel;
import java.sql.Blob;
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
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Created by jianghlu on 8/23/2017.
 *
 * This code monitors a blob for new files every 30 seconds. If there are new files,
 * download them. If there are new files locally, upload them. If there are conflicts,
 * report an error.
 *
 * Also, if there are more than 10 new files in 5 minutes, log it.
 */
public class Scenario {
    private static final long   BLOCK_SIZE  = 100L * 1024 * 1024;
    private static final String URL         = "https://sdkbenchmark.blob.core.windows.net:443";

    private static final List<Blob> remoteFiles = new ArrayList<>();

    private static int count = -1;

    public static void main(String args[]) {
        runRx();
        runFuture();
    }

    public static void runRx() {
        Observable<Blob> stream = Observable.interval(30, TimeUnit.SECONDS)
                .flatMap(l -> listBlobsRx())
                .filter(b -> !remoteFiles.contains(b))
                .doOnNext(remoteFiles::add);

        Observable<Blob> downloads = stream
                .flatMap(b -> {
                    if (b.localCopy() != null) {
                        throw Exceptions.propagate(new Exception("Exists locally"));
                    } else {
                        return downloadBlobRx(b).map(f -> b);
                    }
                });

        Observable<Blob> uploads = stream.map(b -> b.localCopy())
                .toList()
                .flatMap(l -> Observable.from(listDirectory()).filter(l::contains))
                .flatMap(Scenario::uploadBlobRx)
                .onErrorReturn(t -> {
                    if (remoteExistsError(t)) {
                        throw Exceptions.propagate(new Exception("Exists remotely"));
                    } else {
                        throw Exceptions.propagate(t);
                    }
                });

        Observable<Blob> tooManyFiles = stream
                .buffer(stream.debounce(5, TimeUnit.MINUTES))
                .filter(l -> l.size() > 10)
                .flatMap(l -> {
                    log(l.size());
                    return Observable.from(l);
                });

        Observable<?> service = Observable.interval(30, TimeUnit.SECONDS)
                .flatMap(t -> downloads.mergeWith(uploads))
                .mergeWith(tooManyFiles);

    }

    public static void runFuture() {
        Runnable syncs = () -> {
            List<Blob> blobs = null;
            try {
                blobs = listBlobsFuture().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
            for (Blob blob : blobs) {
                if (blob.localCopy() != null) {
                    downloadBlobRx(blob);
                } else {
                    throw new RuntimeException("Exists locally");
                }
            }

            List<File> files = Lists.transform(blobs, b -> b.localCopy());
            for (File file : listDirectory()) {
                if (!files.contains(file)) {
                    try {
                        uploadBlobFuture(file);
                    } catch (Throwable t) {
                        if (remoteExistsError(t)) {
                            throw new RuntimeException("Exists remotely");
                        } else {
                            throw t;
                        }
                    }
                }
            }
        };


        Runnable tooManyFiles = () -> {
            try {
                int size = listBlobsFuture().get().size();
                if (count >= 0 && size - count > 10) {
                    log(size);
                }
                count = size;
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        };

        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(24);

        executor.schedule(syncs, 30, TimeUnit.SECONDS);
        executor.schedule(tooManyFiles, 5, TimeUnit.MINUTES);

        executor.shutdown();
    }

    private static Observable<Blob> listBlobsRx() {
        return null;
    }

    private static ListenableFuture<List<Blob>> listBlobsFuture() {
        return null;
    }

    private static Observable<File> downloadBlobRx(Blob blob) {
        return null;
    }

    private static ListenableFuture<File> downloadBlobFuture(Blob blob) {
        return null;
    }

    private static Observable<Blob> uploadBlobRx(File file) {
        return null;
    }

    private static ListenableFuture<Blob> uploadBlobFuture(File file) {
        return null;
    }

    private static List<File> listDirectory() {
        return null;
    }

    private static class Blob {
        public File localCopy() {
            return null;
        }
    }

    private static boolean remoteExistsError(Throwable t) {
        return false;
    }

    private static void log(Object obj) {

    }
}
