import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import okhttp3.Request;
import okio.Buffer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyClientHandler extends SimpleChannelInboundHandler<HttpContent> {

    //key is sequence ID，value is response message.  
    private Map<Integer,ByteBuf> response = new ConcurrentHashMap<Integer, ByteBuf>();

    //key is sequence ID，value is request thread.  
    private final Map<Integer,Thread> waiters = new ConcurrentHashMap<Integer, Thread>();

    private final AtomicInteger sequence = new AtomicInteger();


    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("client channel is ready!");
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpContent message) throws Exception {
//        ctx.
//        JSONObject json = JSONObject.fromObject(message);
//        Integer id = json.getInt("id");
//        response.put(id,json.getString("md5Hex"));
//
//        Thread thread = waiters.remove(id);
//        synchronized (thread) {
//            thread.notifyAll();
//        }
    }


    public ByteBuf call(Request request, Channel channel) throws Exception {
        int id = sequence.incrementAndGet();
        Thread current = Thread.currentThread();
        waiters.put(id,current);

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
                    request.url().uri().toASCIIString(),
                    Unpooled.wrappedBuffer(buffer.readByteArray()));
        } else {
            fullHttpRequest = new DefaultFullHttpRequest(
                    HttpVersion.HTTP_1_1,
                    HttpMethod.valueOf(request.method()),
                    request.url().uri().toASCIIString());
        }

        fullHttpRequest.headers().set(HttpHeaderNames.HOST, request.url().host());
        fullHttpRequest.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        fullHttpRequest.headers().set(HttpHeaderNames.CONTENT_LENGTH, fullHttpRequest.content().readableBytes());

        channel.writeAndFlush(fullHttpRequest);
        while (!response.containsKey(id)) {
            synchronized (current) {
                current.wait();
            }
        }
        waiters.remove(id);
        return response.remove(id);

    }

}  