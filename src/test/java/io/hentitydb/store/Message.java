package io.hentitydb.store;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

/**
 * A message, ordered in reverse numeric order of id.
 */
public class Message implements Comparable<Message> {

    @JsonProperty
    private final long id;

    @JsonProperty
    private final long threadId;

    @JsonIgnore
    private boolean deleted;

    @JsonProperty
    private Optional<Long> lastSeenMessageId;

    public Message(long id, long threadId) {
        this.id = id;
        this.threadId = threadId;
        this.lastSeenMessageId = Optional.empty();
    }

    @Override
    public int compareTo(Message that) {
        if (that.id > this.id) {
            return 1;
        } else if (that.id < this.id) {
            return -1;
        }
        else {
            return 0;
        }
    }

    public long getId() {
        return id;
    }

    public long getThreadId() {
        return threadId;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    public Optional<Long> getLastSeenMessageId() {
        return lastSeenMessageId;
    }

    public void setLastSeenMessageId(long lastSeenMessageId) {
        this.lastSeenMessageId = Optional.of(lastSeenMessageId);
    }

    public boolean isThreadStarter() {
        return threadId == id;
    }

    @SuppressWarnings("RedundantIfStatement")
    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) return true;

        if (o == null) return false;

        if (!(o instanceof Message)) return false;

        final Message that = (Message) o;
        if (this.id != that.id || this.threadId != that.threadId) return false;

        return true;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("Message(")
                .append(this.id)
                .append(",")
                .append(this.threadId)
                .append(")")
                .toString();
    }

    @Override
    public int hashCode() {
        int result = (int) this.id;
        result = 31 * result + (int)this.threadId;
        return result;
    }
}