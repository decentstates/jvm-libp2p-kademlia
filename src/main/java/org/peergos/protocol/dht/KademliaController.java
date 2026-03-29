package org.peergos.protocol.dht;

import com.google.protobuf.*;
import io.ipfs.multihash.*;
import org.peergos.*;
import org.peergos.protocol.dht.pb.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public interface KademliaController {

    CompletableFuture<Dht.Message> rpc(Dht.Message msg);

    CompletableFuture<Boolean> send(Dht.Message msg);

    default CompletableFuture<List<PeerAddresses>> closerPeers(byte[] key) {
        return rpc(Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.FIND_NODE)
                .setKey(ByteString.copyFrom(key))
                .build())
                .thenApply(resp -> resp.getCloserPeersList().stream()
                        .map(PeerAddresses::fromProtobuf)
                        .collect(Collectors.toList()));
    }

    default CompletableFuture<Boolean> provide(Multihash block, PeerAddresses us) {
        return send(Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.ADD_PROVIDER)
                .setKey(ByteString.copyFrom(block.bareMultihash().toBytes()))
                .addAllProviderPeers(List.of(us.toProtobuf()))
                .build());
    }

    default CompletableFuture<Providers> getProviders(Multihash block) {
        return rpc(Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.GET_PROVIDERS)
                .setKey(ByteString.copyFrom(block.bareMultihash().toBytes()))
                .build())
                .thenApply(Providers::fromProtobuf);
    }

    default CompletableFuture<Boolean> putValue(byte[] key, byte[] value) {
        Dht.Message outgoing = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.PUT_VALUE)
                .setKey(ByteString.copyFrom(key))
                .setRecord(Dht.Record.newBuilder()
                        .setKey(ByteString.copyFrom(key))
                        .setValue(ByteString.copyFrom(value))
                        .build())
                .build();
        return rpc(outgoing).thenApply(reply ->
                reply.getKey().equals(outgoing.getKey()) &&
                reply.getRecord().getValue().equals(outgoing.getRecord().getValue()));
    }

    default CompletableFuture<GetValueResult> getValue(byte[] key) {
        Dht.Message outgoing = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.GET_VALUE)
                .setKey(ByteString.copyFrom(key))
                .build();
        return rpc(outgoing).thenApply(GetValueResult::fromProtobuf);
    }
}
