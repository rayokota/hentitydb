package io.hentitydb.entity;

import com.google.common.base.Objects;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.OrderBy;
import javax.persistence.Table;

@Entity
@Table(name = "test:pinboxes")
@ColumnFamilies({
        @ColumnFamily(name = "i"),
        @ColumnFamily(name = "o",
                maxEntitiesPerRow = 5,
                referencingFamily = "u",
                indexingFamily = "i", indexingValueName = "valueId"),
        @ColumnFamily(name = "u")
})
public class PInboxEntry {
    @ColumnFamilyName
    private String family;
    @Salt
    @Id
    private String inboxId;
    @ElementId
    @OrderBy("DESC")
    private long elementId;
    @Column
    private Long valueId;
    @Column
    private Integer threadType;
    @Column
    private Boolean isPrivate;
    @Column
    private Boolean isRead;
    @Column
    private Boolean isReadAtLeastOnce;
    @Column
    private Boolean isFollowing;
    @Column
    private Long lastReadMessageId;

    // For serialization
    public PInboxEntry() {
    }

    public PInboxEntry(final String family,
                       final String inboxId,
                       final long elementId,
                       final Long valueId,
                       final Integer threadType,
                       final Boolean isPrivate,
                       final Boolean isRead,
                       final Boolean isReadAtLeastOnce,
                       final Boolean isFollowing,
                       final Long lastReadMessageId) {
        this.family = family;
        this.inboxId = inboxId;
        this.elementId = elementId;
        this.valueId = valueId;
        this.threadType = threadType;
        this.isPrivate = isPrivate;
        this.isRead = isRead;
        this.isReadAtLeastOnce = isReadAtLeastOnce;
        this.isFollowing = isFollowing;
        this.lastReadMessageId = lastReadMessageId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PInboxEntry that = (PInboxEntry) o;

        return Objects.equal(family, that.family) &&
                Objects.equal(isFollowing, that.isFollowing) &&
                Objects.equal(isPrivate, that.isPrivate) &&
                Objects.equal(inboxId, that.inboxId) &&
                Objects.equal(elementId, that.elementId) &&
                Objects.equal(valueId, that.valueId) &&
                Objects.equal(threadType, that.threadType) &&
                Objects.equal(isRead, that.isRead) &&
                Objects.equal(isReadAtLeastOnce, that.isReadAtLeastOnce) &&
                Objects.equal(lastReadMessageId, that.lastReadMessageId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(
                family,
                inboxId,
                elementId,
                valueId,
                threadType,
                isPrivate,
                isFollowing,
                isRead,
                isReadAtLeastOnce,
                lastReadMessageId
        );
    }

    @Override
    public String toString() {
        return "InboxEntry{" +
                "family=" + family +
                ", inboxId=" + inboxId +
                ", elementId=" + elementId +
                ", valueId=" + valueId +
                ", threadType=" + threadType +
                ", isPrivate=" + isPrivate +
                ", isRead=" + isRead +
                ", isReadAtLeastOnce=" + isReadAtLeastOnce +
                ", isFollowing=" + isFollowing +
                ", lastReadMessageId=" + lastReadMessageId +
                '}';
    }

    public String getFamily() {
        return family;
    }

    public String getInboxId() {
        return inboxId;
    }

    public Long getElementId() {
        return elementId;
    }

    public Long getValueId() {
        return valueId;
    }

    public Boolean isFollowing() {
        return isFollowing;
    }

    public Integer getThreadType() {
        return threadType;
    }

    public Boolean isPrivate() {
        return isPrivate;
    }

    public Boolean isRead() {
        return isRead;
    }

    public Boolean isReadAtLeastOnce() {
        return isReadAtLeastOnce;
    }

    public Long getLastReadMessageId() {
        return lastReadMessageId;
    }
}
