package org.peergos.protocol.dht;

import com.google.protobuf.ByteString;
import org.peergos.PeerAddresses;
import org.peergos.protocol.dht.pb.Dht;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class GetValueResult {
    /** The raw record value, if the remote peer held one. */
    public final Optional<byte[]> value;
    /** Peers closer to the key, as returned by the remote. */
    public final List<PeerAddresses> closerPeers;

    public GetValueResult(Optional<byte[]> value, List<PeerAddresses> closerPeers) {
        this.value = value;
        this.closerPeers = closerPeers;
    }

    public static GetValueResult fromProtobuf(Dht.Message msg) {
        Optional<byte[]> value = msg.hasRecord() && !msg.getRecord().getValue().equals(ByteString.EMPTY)
                ? Optional.of(msg.getRecord().getValue().toByteArray())
                : Optional.empty();
        List<PeerAddresses> closer = msg.getCloserPeersList().stream()
                .map(PeerAddresses::fromProtobuf)
                .collect(Collectors.toList());
        return new GetValueResult(value, closer);
    }
}
