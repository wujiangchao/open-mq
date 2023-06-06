

package com.open.openmq.common.rpchook;


import com.open.openmq.common.MixAll;
import com.open.openmq.remoting.RPCHook;
import com.open.openmq.remoting.protocol.RemotingCommand;
import com.open.openmq.remoting.protocol.RequestType;

/**
 * 消息流类型是RocketMQ中用于区分消息不同类型的一种机制。它允许用户在发送消息时，将消息的类型作为一个参数传递出去，以便在消费者端进行解析和使用。
 * StreamTypeRPCHook类的作用是在发送消息时，对消息的消息流类型进行处理。通过实现这个RPC钩子，用户可以在发送消息之前或之后执行自定义逻辑，
 * 例如对消息流类型进行加密、压缩或验证等操作。这使得用户可以更灵活地处理消息流类型，并在必要时对它们进行自定义处理。
 */
public class StreamTypeRPCHook implements RPCHook {
    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        request.addExtField(MixAll.REQ_T, String.valueOf(RequestType.STREAM.getCode()));
    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request,
        RemotingCommand response) {

    }
}
