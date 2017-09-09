import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Buffer;
import okio.BufferedSource;
import okio.Okio;
import org.joda.time.DateTime;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by jianghlu on 7/20/17.
 */
public class NettyCallFactory implements Call.Factory {
    EventLoopGroup workerGroup = new NioEventLoopGroup();
    Bootstrap bootstrap;
    SslContext sslContext;

    public NettyCallFactory() {
        bootstrap = new Bootstrap();
        bootstrap.group(workerGroup);
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
                if (sslContext != null) {
                    ch.pipeline().addLast(sslContext.newHandler(ch.alloc(), "https://sdkbenchmark.blob.core.windows.net", 443));
                }
                ch.pipeline().addLast(new HttpResponseDecoder());
                ch.pipeline().addLast(new HttpRequestEncoder());
                ch.pipeline().addLast(new HttpClientInboundHandler());
            }
        });
    }

    @Override
    public Call newCall(final Request request) {
        return new Call() {
            private boolean executed = false;

            @Override
            public Request request() {
                return request;
            }

            @Override
            public Response execute() throws IOException {
                try {
                    executed = true;
                    // Start the client.
                    ChannelFuture future = bootstrap.connect(request.url().host(), request.url().port()).sync();

                    Buffer buffer = null;
                    if (request.body() != null) {
                        buffer = new Buffer();
                        request.body().writeTo(buffer);
                    }

                    DefaultFullHttpRequest fullHttpRequest;
                    if (buffer != null) {
                        fullHttpRequest = new DefaultFullHttpRequest(
                                HttpVersion.HTTP_1_1,
                                HttpMethod.valueOf(request.method()),
                                request().url().uri().toASCIIString(),
                                Unpooled.wrappedBuffer(buffer.readByteArray()));
                    } else {
                        fullHttpRequest = new DefaultFullHttpRequest(
                                HttpVersion.HTTP_1_1,
                                HttpMethod.valueOf(request.method()),
                                request().url().uri().toASCIIString());
                    }

                    fullHttpRequest.headers().set(HttpHeaderNames.HOST, request.url().host());
                    fullHttpRequest.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
                    fullHttpRequest.headers().set(HttpHeaderNames.CONTENT_LENGTH, fullHttpRequest.content().readableBytes());

                    HttpClientInboundHandler handler = (HttpClientInboundHandler) future.channel().pipeline().last();
                    StreamingResponse res = handler.send(future.channel(), fullHttpRequest);
                    BufferedSource src = Okio.buffer(Okio.source(res.getStream()));

                    return new Response.Builder()
                            .protocol(Protocol.HTTP_1_1)
                            .request(request)
                            .code(200)
                            .body(ResponseBody.create(MediaType.parse(
                                    res.contentType),
                                    res.contentLength,
                                    src)).build();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void enqueue(Callback responseCallback) {
                throw new NotImplementedException();
            }

            @Override
            public void cancel() {
                throw new NotImplementedException();
            }

            @Override
            public boolean isExecuted() {
                return executed;
            }

            @Override
            public boolean isCanceled() {
                return false;
            }
        };
    }

    public class HttpClientInboundHandler extends ChannelInboundHandlerAdapter {
        private final Map<ChannelId, StreamingResponse> responses = new ConcurrentHashMap<ChannelId, StreamingResponse>();

        private final Map<ChannelId, CountDownLatch> latches = new ConcurrentHashMap<ChannelId, CountDownLatch>();

        public StreamingResponse send(Channel channel, FullHttpRequest httpRequest) {
            channel.writeAndFlush(httpRequest);
            channel.read();
            CountDownLatch latch = new CountDownLatch(1);
            latches.put(channel.id(), latch);
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return responses.get(channel.id());
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof HttpResponse)
            {
                HttpResponse response = (HttpResponse) msg;
                StreamingResponse res = new StreamingResponse();
                res.contentType = response.headers().get(HttpHeaderNames.CONTENT_TYPE);
                res.contentLength = Long.parseLong(response.headers().get(HttpHeaderNames.CONTENT_LENGTH));
                responses.put(ctx.channel().id(), res);
                CountDownLatch latch = latches.remove(ctx.channel().id());
                latch.countDown();
            }
            if(msg instanceof HttpContent)
            {
                System.out.println("- Receiving! " + DateTime.now().getMillis());
                HttpContent content = (HttpContent)msg;
                System.out.println("Adding! " + DateTime.now().getMillis());
                responses.get(ctx.channel().id()).addBuf(content.content());
                System.out.println("Added! " + DateTime.now().getMillis());
            }
        }
    }

    public class StreamingResponse {
        private String contentType;
        private long contentLength;
        private BlockingQueue<ByteBuf> bufQueue = new LinkedBlockingDeque<ByteBuf>();

        private AtomicLong received = new AtomicLong();

        private void addBuf(ByteBuf b) {
            bufQueue.add(b);
            received.addAndGet(b.readableBytes());
        }

        public InputStream getStream() {
            if (contentLength == 0) {
                return new InputStream() {
                    @Override
                    public int read() throws IOException {
                        return -1;
                    }
                };
            }

            return new InputStream() {
                ByteBuf current;
                @Override
                public int read() throws IOException {
                    if (current == null) {
                        current = getNextBuf();
                    }
                    if (current.isReadable()) {
                        return current.readByte();
                    } else {
//                        current.release();
                        if (bufQueue.size() > 0 || received.get() < contentLength) {
                            current = getNextBuf();
                            return current.readByte();
                        } else {
                            System.out.println("Done receiving! " + DateTime.now().getMillis());
                            return -1;
                        }
                    }
                }
            };
        }

        public ByteBuf getNextBuf() {
            boolean interrupted = false;
            try {
                for (;;) {
                    try {
                        return bufQueue.take();
                    } catch (InterruptedException ignore) {
                        interrupted = true;
                    }
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
