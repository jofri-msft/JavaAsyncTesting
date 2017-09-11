import com.google.common.util.concurrent.SettableFuture;
import com.microsoft.azure.CloudException;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.proxy.HttpProxyHandler;
import io.netty.handler.proxy.ProxyHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.ReferenceCountUtil;
import rx.Emitter;
import rx.Emitter.BackpressureMode;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.exceptions.Exceptions;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.schedulers.Schedulers;
import rx.subjects.AsyncSubject;
import rx.subjects.PublishSubject;
import rx.subjects.ReplaySubject;

import javax.net.ssl.SSLException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jianghlu on 7/20/17.
 */
public class NettyRxAdapter {
    private Bootstrap bootstrap;
    private SslContext sslContext;
    private final static AtomicInteger CAP = new AtomicInteger(24);

    private final static AtomicInteger THROTTLER = new AtomicInteger(800);

    public NettyRxAdapter() {
        bootstrap = new Bootstrap();
        bootstrap.group(new NioEventLoopGroup());
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        try {
            sslContext = SslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        } catch (SSLException e) {
            e.printStackTrace();
        }
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                //if ()
//                if (sslContext != null) {
//                    ch.pipeline().addLast(sslContext.newHandler(ch.alloc(), "http://xclientdev2.blob.core.windows.net", 80));
//                }
                //ch.pipeline().addLast()

                ch.pipeline().addLast(new HttpResponseDecoder());
                ch.pipeline().addLast(new HttpRequestEncoder());
                ch.pipeline().addLast(new RetryChannelHandler());
                ch.pipeline().addLast(new HttpClientInboundHandler());
                ch.pipeline().addFirst((new HttpProxyHandler(new InetSocketAddress("localhost", 8888))));
            }
        });
    }

    public static void throttle() {
        while (THROTTLER.get() <= 0) {
            synchronized (THROTTLER) {
                try {
                    THROTTLER.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        THROTTLER.decrementAndGet();
    }

    public static void release() {
        THROTTLER.incrementAndGet();
        synchronized (THROTTLER) {
            THROTTLER.notify();
        }
    }

    public Observable<ByteBuf> sendRequestAsync(DefaultFullHttpRequest request) {
        return Observable.defer(() -> {
            int retry = 0;

            while (true) {
                try {
                    URI uri = new URI(request.uri());

                    throttle();

                    ChannelFuture future = bootstrap.connect(uri.getHost(), uri.getPort());

                    return Observable.<Observable<ByteBuf>>fromEmitter(emitter -> future.addListener(cf -> {
                        emitter.onNext(((HttpClientInboundHandler) future.channel().pipeline().last()).subject);
                        emitter.onCompleted();

                        System.out.println("Sending...");
                        request.headers().set(HttpHeaderNames.HOST, uri.getHost());
                        request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
//                    request.headers().set(HttpHeaderNames.CONTENT_LENGTH, request.content().readableBytes());

                        future.channel().writeAndFlush(request).addListener(f -> {
                            System.out.println("Sent");
                            future.channel().closeFuture();
                        });
                    }), BackpressureMode.BUFFER).flatMap(o -> o)
                            .retryWhen(observable -> observable.zipWith(Observable.range(1, 30), (throwable, integer) -> integer)
                                    .flatMap(i -> Observable.timer(i, TimeUnit.SECONDS)));
                } catch (Throwable e) {
                    retry ++;
                    if (retry == 30) {
                        return Observable.error(e);
                    } else {
                        try {
                            Thread.sleep(retry * 1000);
                        } catch (InterruptedException e1) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }
        });
    }

    public static class HttpClientInboundHandler extends ChannelInboundHandlerAdapter {

        private static final String HEADER_CONTENT_LENGTH = "Content-Length";
//        private Emitter<ByteBuf> emitter;
//
//        private Observable<ByteBuf> observable;

        ReplaySubject<ByteBuf> subject;

        private long contentLength;

        public HttpClientInboundHandler() {
//            observable = Observable.fromEmitter(emitter -> HttpClientInboundHandler.this.emitter = emitter, BackpressureMode.BUFFER);
            subject = ReplaySubject.create();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof HttpResponse)
            {
                System.out.println("Incoming response...");
                release();
                HttpResponse response = (HttpResponse) msg;
                if (response.headers().contains(HEADER_CONTENT_LENGTH)) {
                    contentLength = Long.parseLong(response.headers().get(HEADER_CONTENT_LENGTH));
                }
            }
            if(msg instanceof HttpContent)
            {
                HttpContent content = (HttpContent)msg;
                ByteBuf buf = content.content();

                if (contentLength == 0) {
                    subject.onNext(null);
                } else if (buf != null && buf.readableBytes() > 0) {
                    contentLength -= buf.readableBytes();
                    subject.onNext(buf);
                }

                if (contentLength == 0) {
                    subject.onCompleted();
                }
            }
        }
    }

    public static class RetryChannelHandler extends ChannelDuplexHandler {
        Queue<HttpObject> requestParts;

        int retry = 0;

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            HttpObject dup;
            if (msg instanceof HttpContent) {
                dup = ((HttpContent) msg).duplicate().retain();
            } else {
                dup = (HttpObject) msg;
            }

            if (msg instanceof HttpRequest) {
                requestParts = new ArrayDeque<>();
                requestParts.add(dup);
            } else if (msg instanceof HttpContent) {
                requestParts.add(dup);
            }

            super.write(ctx, msg, promise);
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof HttpResponse) {
                HttpResponse res = (HttpResponse)msg;
                if (res.status().code() == 503 && retry <= 10) {
                    retry ++;
                    ctx.executor().schedule(() -> {
                        HttpObject obj;
                        while ((obj = requestParts.poll()) != null) {
                            ctx.write(obj);
                        }
                        ctx.flush();
                    }, retry, TimeUnit.SECONDS);
                } else {
                    HttpObject obj;
                    while ((obj = requestParts.poll()) != null) {
                        if (obj instanceof HttpContent) {
                            ReferenceCountUtil.release(obj);
                        }
                    }
                    super.channelRead(ctx, msg);
                }
            } else {
                super.channelRead(ctx, msg);
            }
        }
    }

}
