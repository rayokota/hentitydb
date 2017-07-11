package io.hentitydb.store;

import io.hentitydb.serialization.AbstractVersionedCodec;
import io.hentitydb.serialization.ReadBuffer;
import io.hentitydb.serialization.WriteBuffer;

import java.util.Optional;

public class MessageCodec extends AbstractVersionedCodec<Message> {
    public static final MessageCodec INSTANCE = new MessageCodec();

    private static final long NO_LAST_VIEWED_MESSAGE_ID = -1;

    @Override
    public int getLatestVersion() {
        return 1;
    }

    @Override
    protected void encode(int version, final Message value, final WriteBuffer buffer) {
        buffer.writeVarLong(value.getId());
        buffer.writeVarLong(value.getThreadId());
        Optional<Long> lastViewedMessageId = value.getLastSeenMessageId();
        buffer.writeVarLong(lastViewedMessageId.isPresent() ? lastViewedMessageId.get() : NO_LAST_VIEWED_MESSAGE_ID);
        buffer.writeByte(value.isDeleted() ? 1 : 0);
    }

    @Override
    protected Message decode(int version, final ReadBuffer buffer) {
        final long id = buffer.readVarLong();
        final long threadId = buffer.readVarLong();
        final long lastViewedMessageId = buffer.readVarLong();
        final boolean isDeleted = buffer.readByte() == 1;
        Message message = new Message(id, threadId);
        if (lastViewedMessageId != NO_LAST_VIEWED_MESSAGE_ID) {
            message.setLastSeenMessageId(lastViewedMessageId);
        }
        if (isDeleted) {
            message.setDeleted(true);
        }
        return message;
    }
}
