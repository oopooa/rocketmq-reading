/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.remoting.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.opentelemetry.api.common.AttributesBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.rocketmq.common.AbortProcessException;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.metrics.RemotingMetricsManager;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_IS_LONG_POLLING;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_REQUEST_CODE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESPONSE_CODE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESULT;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.RESULT_ONEWAY;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.RESULT_PROCESS_REQUEST_FAILED;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.RESULT_WRITE_CHANNEL_FAILED;

public abstract class NettyRemotingAbstract {

    /**
     * Remoting logger instance.
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);

    /**
     * Semaphore to limit maximum number of on-going one-way requests, which protects system memory footprint.
     */
    protected final Semaphore semaphoreOneway;

    /**
     * Semaphore to limit maximum number of on-going asynchronous requests, which protects system memory footprint.
     */
    protected final Semaphore semaphoreAsync;

    /**
     * This map caches all on-going requests.
     */
    protected final ConcurrentMap<Integer /* opaque */, ResponseFuture> responseTable =
        new ConcurrentHashMap<>(256);

    /**
     * This container holds all processors per request code, aka, for each incoming request, we may look up the
     * responding processor in this map to handle the request.
     */
    protected final HashMap<Integer/* request code */, Pair<NettyRequestProcessor, ExecutorService>> processorTable =
        new HashMap<>(64);

    /**
     * Executor to feed netty events to user defined {@link ChannelEventListener}.
     */
    protected final NettyEventExecutor nettyEventExecutor = new NettyEventExecutor();

    /**
     * The default request processor to use in case there is no exact match in {@link #processorTable} per request
     * code.
     */
    protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessorPair;

    /**
     * SSL context via which to create {@link SslHandler}.
     */
    protected volatile SslContext sslContext;

    /**
     * custom rpc hooks
     */
    protected List<RPCHook> rpcHooks = new ArrayList<>();

    protected AtomicBoolean isShuttingDown = new AtomicBoolean(false);

    static {
        NettyLogger.initNettyLogger();
    }

    /**
     * Constructor, specifying capacity of one-way and asynchronous semaphores.
     *
     * @param permitsOneway Number of permits for one-way requests.
     * @param permitsAsync  Number of permits for asynchronous requests.
     */
    public NettyRemotingAbstract(final int permitsOneway, final int permitsAsync) {
        this.semaphoreOneway = new Semaphore(permitsOneway, true);
        this.semaphoreAsync = new Semaphore(permitsAsync, true);
    }

    /**
     * Custom channel event listener.
     *
     * @return custom channel event listener if defined; null otherwise.
     */
    public abstract ChannelEventListener getChannelEventListener();

    /**
     * Put a netty event to the executor.
     *
     * @param event Netty event instance.
     */
    public void putNettyEvent(final NettyEvent event) {
        this.nettyEventExecutor.putNettyEvent(event);
    }

    /**
     * 处理 RemotingCommand 命令消息, 传入的远程命令可能是：
     * <ul>
     * <li>1. 远程对等组件的查询请求</li>
     * <li>2. 该参与者之前发出请求的响应</li>
     * </ul>
     */
    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) {
        if (msg != null) {
            switch (msg.getType()) {
                // 处理来源服务器端的请求命令
                case REQUEST_COMMAND:
                    processRequestCommand(ctx, msg);
                    break;
                // 处理来源服务器端的响应命令
                case RESPONSE_COMMAND:
                    processResponseCommand(ctx, msg);
                    break;
                default:
                    break;
            }
        }
    }

    protected void doBeforeRpcHooks(String addr, RemotingCommand request) {
        if (rpcHooks.size() > 0) {
            for (RPCHook rpcHook : rpcHooks) {
                rpcHook.doBeforeRequest(addr, request);
            }
        }
    }

    public void doAfterRpcHooks(String addr, RemotingCommand request, RemotingCommand response) {
        if (rpcHooks.size() > 0) {
            for (RPCHook rpcHook : rpcHooks) {
                rpcHook.doAfterResponse(addr, request, response);
            }
        }
    }

    public static void writeResponse(Channel channel, RemotingCommand request, @Nullable RemotingCommand response) {
        writeResponse(channel, request, response, null);
    }

    public static void writeResponse(Channel channel, RemotingCommand request, @Nullable RemotingCommand response,
        Consumer<Future<?>> callback) {
        if (response == null) {
            return;
        }
        // 构建指标数据 (是否长轮询, 请求码, 响应码)
        AttributesBuilder attributesBuilder = RemotingMetricsManager.newAttributesBuilder()
            .put(LABEL_IS_LONG_POLLING, request.isSuspended())
            .put(LABEL_REQUEST_CODE, RemotingHelper.getRequestCodeDesc(request.getCode()))
            .put(LABEL_RESPONSE_CODE, RemotingHelper.getResponseCodeDesc(response.getCode()));
        // 如果是单向消息
        if (request.isOnewayRPC()) {
            attributesBuilder.put(LABEL_RESULT, RESULT_ONEWAY);
            // 记录指标数据
            RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
            // 不发送响应
            return;
        }
        // 设置请求 id
        response.setOpaque(request.getOpaque());
        response.markResponseType();
        try {
            // 将响应写入到 channel 缓冲区并立即发送到远程节点。并针对这个异步操作, 添加一个监听器用于完成时回调
            channel.writeAndFlush(response).addListener((ChannelFutureListener) future -> {
                // 日志记录
                if (future.isSuccess()) {
                    log.debug("Response[request code: {}, response code: {}, opaque: {}] is written to channel{}",
                        request.getCode(), response.getCode(), response.getOpaque(), channel);
                } else {
                    log.error("Failed to write response[request code: {}, response code: {}, opaque: {}] to channel{}",
                        request.getCode(), response.getCode(), response.getOpaque(), channel, future.cause());
                }
                // 设置响应结果
                attributesBuilder.put(LABEL_RESULT, RemotingMetricsManager.getWriteAndFlushResult(future));
                // 记录指标数据
                RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
                // 如果回调不为空
                if (callback != null) {
                    // 执行回调
                    callback.accept(future);
                }
            });
        } catch (Throwable e) {
            log.error("process request over, but response failed", e);
            log.error(request.toString());
            log.error(response.toString());
            // 设置指标结果为写入通道失败
            attributesBuilder.put(LABEL_RESULT, RESULT_WRITE_CHANNEL_FAILED);
            // 记录指标数据
            RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
        }
    }

    /**
     * Process incoming request command issued by remote peer.
     *
     * @param ctx channel handler context.
     * @param cmd request command.
     */
    public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        // 根据请求 code 在本地缓存变量中寻找对应的请求处理器以及线程池资源
        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
        // 如果没找到对应的处理器, 则返回一个默认的请求处理器
        final Pair<NettyRequestProcessor, ExecutorService> pair = null == matched ? this.defaultRequestProcessorPair : matched;
        // 获取请求唯一 id, 从 0 开始自增
        final int opaque = cmd.getOpaque();

        /**
         * ❓ 如果请求 code 不在本地变量中, matched 为 null, 则 pair = this.defaultRequestProcessorPair, 后者在应用启动时
         * 会被注册, 也不会为空, 不清楚什么情况下 pair 为会 null
         */
        if (pair == null) {
            // 构建异常信息
            String error = " request type " + cmd.getCode() + " not supported";
            // 创建请求码不支持的异常响应
            final RemotingCommand response =
                RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
            // 设置请求 id
            response.setOpaque(opaque);
            // 返回响应
            writeResponse(ctx.channel(), cmd, response);
            log.error(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + error);
            return;
        }

        // ❓ (感觉构建方法可以往下移, 避免构建完之后还没提交就判断退出了) 构建请求处理函数, 并不实际执行, 后续交由对应线程池执行
        Runnable run = buildProcessRequestHandler(ctx, cmd, pair, opaque);

        // 如果服务正在终止
        if (isShuttingDown.get()) {
            if (cmd.getVersion() > MQVersion.Version.V5_1_4.ordinal()) {
                // 构建 go away 响应
                final RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.GO_AWAY,
                    "please go away");
                // 设置请求 id
                response.setOpaque(opaque);
                // 返回响应, 让客户端重新选一个 channel 请求
                writeResponse(ctx.channel(), cmd, response);
                return;
            }
        }

        // 判断执行器是否拒绝处理该请求
        if (pair.getObject1().rejectRequest()) {
            // 构建系统繁忙响应
            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                "[REJECTREQUEST]system busy, start flow control for a while");
            // 设置请求 id
            response.setOpaque(opaque);
            // 返回响应
            writeResponse(ctx.channel(), cmd, response);
            return;
        }

        try {
            // 构建请求任务
            final RequestTask requestTask = new RequestTask(run, ctx.channel(), cmd);
            // 提交到线程池异步执行任务, 当前线程直接返回
            pair.getObject2().submit(requestTask);
        } catch (RejectedExecutionException e) {
            if ((System.currentTimeMillis() % 10000) == 0) {
                log.warn(RemotingHelper.parseChannelRemoteAddr(ctx.channel())
                    + ", too many requests and system thread pool busy, RejectedExecutionException "
                    + pair.getObject2().toString()
                    + " request code: " + cmd.getCode());
            }

            // 构建系统繁忙响应
            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                "[OVERLOAD]system busy, start flow control for a while");
            response.setOpaque(opaque);
            // 返回响应
            writeResponse(ctx.channel(), cmd, response);
        } catch (Throwable e) {
            AttributesBuilder attributesBuilder = RemotingMetricsManager.newAttributesBuilder()
                .put(LABEL_REQUEST_CODE, RemotingHelper.getRequestCodeDesc(cmd.getCode()))
                .put(LABEL_RESULT, RESULT_PROCESS_REQUEST_FAILED);
            RemotingMetricsManager.rpcLatency.record(cmd.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
        }
    }

    private Runnable buildProcessRequestHandler(ChannelHandlerContext ctx, RemotingCommand cmd,
        Pair<NettyRequestProcessor, ExecutorService> pair, int opaque) {
        return () -> {
            Exception exception = null;
            RemotingCommand response;

            try {
                // 获取通道远程地址
                String remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                try {
                    // 执行前置钩子函数
                    doBeforeRpcHooks(remoteAddr, cmd);
                } catch (AbortProcessException e) {
                    throw e;
                } catch (Exception e) {
                    exception = e;
                }

                // 如果前置函数没有发生异常
                if (exception == null) {
                    // 获取对应请求执行器执行请求
                    response = pair.getObject1().processRequest(ctx, cmd);
                } else {
                    // 前置函数发生异常, 构建系统异常响应
                    response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, null);
                }

                try {
                    // 执行后置钩子函数
                    doAfterRpcHooks(remoteAddr, cmd, response);
                } catch (AbortProcessException e) {
                    throw e;
                } catch (Exception e) {
                    exception = e;
                }

                // 如果有异常, 则抛出
                if (exception != null) {
                    throw exception;
                }

                // 返回响应到通道
                writeResponse(ctx.channel(), cmd, response);
            } catch (AbortProcessException e) {
                response = RemotingCommand.createResponseCommand(e.getResponseCode(), e.getErrorMessage());
                response.setOpaque(opaque);
                writeResponse(ctx.channel(), cmd, response);
            } catch (Throwable e) {
                log.error("process request exception", e);
                log.error(cmd.toString());

                // 如果不是单向消息
                if (!cmd.isOnewayRPC()) {
                    // 构建异常响应
                    response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR,
                        UtilAll.exceptionSimpleDesc(e));
                    // 设置请求 id
                    response.setOpaque(opaque);
                    // 返回响应
                    writeResponse(ctx.channel(), cmd, response);
                }
            }
        };
    }

    /**
     * Process response from remote peer to the previous issued requests.
     *
     * @param ctx channel handler context.
     * @param cmd response command instance.
     */
    public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        final int opaque = cmd.getOpaque();
        final ResponseFuture responseFuture = responseTable.get(opaque);
        if (responseFuture != null) {
            responseFuture.setResponseCommand(cmd);

            responseTable.remove(opaque);

            if (responseFuture.getInvokeCallback() != null) {
                executeInvokeCallback(responseFuture);
            } else {
                responseFuture.putResponse(cmd);
                responseFuture.release();
            }
        } else {
            log.warn("receive response, cmd={}, but not matched any request, address={}", cmd, RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        }
    }

    /**
     * Execute callback in callback executor. If callback executor is null, run directly in current thread
     */
    private void executeInvokeCallback(final ResponseFuture responseFuture) {
        boolean runInThisThread = false;
        ExecutorService executor = this.getCallbackExecutor();
        if (executor != null && !executor.isShutdown()) {
            try {
                executor.submit(() -> {
                    try {
                        responseFuture.executeInvokeCallback();
                    } catch (Throwable e) {
                        log.warn("execute callback in executor exception, and callback throw", e);
                    } finally {
                        responseFuture.release();
                    }
                });
            } catch (Exception e) {
                runInThisThread = true;
                log.warn("execute callback in executor exception, maybe executor busy", e);
            }
        } else {
            runInThisThread = true;
        }

        if (runInThisThread) {
            try {
                responseFuture.executeInvokeCallback();
            } catch (Throwable e) {
                log.warn("executeInvokeCallback Exception", e);
            } finally {
                responseFuture.release();
            }
        }
    }

    /**
     * Custom RPC hooks.
     *
     * @return RPC hooks if specified; null otherwise.
     */
    public List<RPCHook> getRPCHook() {
        return rpcHooks;
    }

    public void registerRPCHook(RPCHook rpcHook) {
        if (rpcHook != null && !rpcHooks.contains(rpcHook)) {
            rpcHooks.add(rpcHook);
        }
    }

    public void clearRPCHook() {
        rpcHooks.clear();
    }

    /**
     * This method specifies thread pool to use while invoking callback methods.
     *
     * @return Dedicated thread pool instance if specified; or null if the callback is supposed to be executed in the
     * netty event-loop thread.
     */
    public abstract ExecutorService getCallbackExecutor();

    /**
     * <p>
     * This method is periodically invoked to scan and expire deprecated request.
     * </p>
     */
    public void scanResponseTable() {
        final List<ResponseFuture> rfList = new LinkedList<>();
        Iterator<Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, ResponseFuture> next = it.next();
            ResponseFuture rep = next.getValue();

            if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000) <= System.currentTimeMillis()) {
                rep.release();
                it.remove();
                rfList.add(rep);
                log.warn("remove timeout request, " + rep);
            }
        }

        for (ResponseFuture rf : rfList) {
            try {
                executeInvokeCallback(rf);
            } catch (Throwable e) {
                log.warn("scanResponseTable, operationComplete Exception", e);
            }
        }
    }

    public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request,
        final long timeoutMillis)
        throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        try {
            return invokeImpl(channel, request, timeoutMillis).thenApply(ResponseFuture::getResponseCommand)
                .get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new RemotingSendRequestException(channel.remoteAddress().toString(), e.getCause());
        } catch (TimeoutException e) {
            throw new RemotingTimeoutException(channel.remoteAddress().toString(), timeoutMillis, e.getCause());
        }
    }

    public CompletableFuture<ResponseFuture> invokeImpl(final Channel channel, final RemotingCommand request,
        final long timeoutMillis) {
        String channelRemoteAddr = RemotingHelper.parseChannelRemoteAddr(channel);
        doBeforeRpcHooks(channelRemoteAddr, request);
        return invoke0(channel, request, timeoutMillis).whenComplete((v, t) -> {
            if (t == null) {
                doAfterRpcHooks(channelRemoteAddr, request, v.getResponseCommand());
            }
        });
    }

    protected CompletableFuture<ResponseFuture> invoke0(final Channel channel, final RemotingCommand request,
        final long timeoutMillis) {
        CompletableFuture<ResponseFuture> future = new CompletableFuture<>();
        long beginStartTime = System.currentTimeMillis();
        final int opaque = request.getOpaque();

        boolean acquired;
        try {
            acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            future.completeExceptionally(t);
            return future;
        }
        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);
            long costTime = System.currentTimeMillis() - beginStartTime;
            if (timeoutMillis < costTime) {
                once.release();
                future.completeExceptionally(new RemotingTimeoutException("invokeAsyncImpl call timeout"));
                return future;
            }

            AtomicReference<ResponseFuture> responseFutureReference = new AtomicReference<>();
            final ResponseFuture responseFuture = new ResponseFuture(channel, opaque, request, timeoutMillis - costTime,
                new InvokeCallback() {
                    @Override
                    public void operationComplete(ResponseFuture responseFuture) {

                    }

                    @Override
                    public void operationSucceed(RemotingCommand response) {
                        future.complete(responseFutureReference.get());
                    }

                    @Override
                    public void operationFail(Throwable throwable) {
                        future.completeExceptionally(throwable);
                    }
                }, once);
            responseFutureReference.set(responseFuture);
            this.responseTable.put(opaque, responseFuture);
            try {
                channel.writeAndFlush(request).addListener((ChannelFutureListener) f -> {
                    if (f.isSuccess()) {
                        responseFuture.setSendRequestOK(true);
                        return;
                    }
                    requestFail(opaque);
                    log.warn("send a request command to channel <{}> failed.", RemotingHelper.parseChannelRemoteAddr(channel));
                });
                return future;
            } catch (Exception e) {
                responseTable.remove(opaque);
                responseFuture.release();
                log.warn("send a request command to channel <" + RemotingHelper.parseChannelRemoteAddr(channel) + "> Exception", e);
                future.completeExceptionally(new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e));
                return future;
            }
        } else {
            if (timeoutMillis <= 0) {
                future.completeExceptionally(new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fast"));
            } else {
                String info =
                    String.format("invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d",
                        timeoutMillis,
                        this.semaphoreAsync.getQueueLength(),
                        this.semaphoreAsync.availablePermits()
                    );
                log.warn(info);
                future.completeExceptionally(new RemotingTimeoutException(info));
            }
            return future;
        }
    }

    public void invokeAsyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis,
        final InvokeCallback invokeCallback) {
        invokeImpl(channel, request, timeoutMillis)
            .whenComplete((v, t) -> {
                if (t == null) {
                    invokeCallback.operationComplete(v);
                } else {
                    ResponseFuture responseFuture = new ResponseFuture(channel, request.getOpaque(), request, timeoutMillis, null, null);
                    responseFuture.setCause(t);
                    invokeCallback.operationComplete(responseFuture);
                }
            })
            .thenAccept(responseFuture -> invokeCallback.operationSucceed(responseFuture.getResponseCommand()))
            .exceptionally(t -> {
                invokeCallback.operationFail(t);
                return null;
            });
    }

    private void requestFail(final int opaque) {
        ResponseFuture responseFuture = responseTable.remove(opaque);
        if (responseFuture != null) {
            responseFuture.setSendRequestOK(false);
            responseFuture.putResponse(null);
            try {
                executeInvokeCallback(responseFuture);
            } catch (Throwable e) {
                log.warn("execute callback in requestFail, and callback throw", e);
            } finally {
                responseFuture.release();
            }
        }
    }

    /**
     * mark the request of the specified channel as fail and to invoke fail callback immediately
     *
     * @param channel the channel which is close already
     */
    protected void failFast(final Channel channel) {
        for (Entry<Integer, ResponseFuture> entry : responseTable.entrySet()) {
            if (entry.getValue().getChannel() == channel) {
                Integer opaque = entry.getKey();
                if (opaque != null) {
                    requestFail(opaque);
                }
            }
        }
    }

    public void invokeOnewayImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        request.markOnewayRPC();
        boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);
            try {
                channel.writeAndFlush(request).addListener((ChannelFutureListener) f -> {
                    once.release();
                    if (!f.isSuccess()) {
                        log.warn("send a request command to channel <" + channel.remoteAddress() + "> failed.");
                    }
                });
            } catch (Exception e) {
                once.release();
                log.warn("write send a request command to channel <" + channel.remoteAddress() + "> failed.");
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeOnewayImpl invoke too fast");
            } else {
                String info = String.format(
                    "invokeOnewayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreOnewayValue: %d",
                    timeoutMillis,
                    this.semaphoreOneway.getQueueLength(),
                    this.semaphoreOneway.availablePermits()
                );
                log.warn(info);
                throw new RemotingTimeoutException(info);
            }
        }
    }

    class NettyEventExecutor extends ServiceThread {
        private final LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<>();

        public void putNettyEvent(final NettyEvent event) {
            int currentSize = this.eventQueue.size();
            int maxSize = 10000;
            if (currentSize <= maxSize) {
                this.eventQueue.add(event);
            } else {
                log.warn("event queue size [{}] over the limit [{}], so drop this event {}", currentSize, maxSize, event.toString());
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            final ChannelEventListener listener = NettyRemotingAbstract.this.getChannelEventListener();

            while (!this.isStopped()) {
                try {
                    NettyEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                    if (event != null && listener != null) {
                        switch (event.getType()) {
                            case IDLE:
                                listener.onChannelIdle(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CLOSE:
                                listener.onChannelClose(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CONNECT:
                                listener.onChannelConnect(event.getRemoteAddr(), event.getChannel());
                                break;
                            case EXCEPTION:
                                listener.onChannelException(event.getRemoteAddr(), event.getChannel());
                                break;
                            case ACTIVE:
                                listener.onChannelActive(event.getRemoteAddr(), event.getChannel());
                                break;
                            default:
                                break;

                        }
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return NettyEventExecutor.class.getSimpleName();
        }
    }
}
