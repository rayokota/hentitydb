# HEntityDB - HBase as an Entity Database

[![Build Status][github-actions-shield]][github-actions-link]
[![Maven][maven-shield]][maven-link]
[![Javadoc][javadoc-shield]][javadoc-link]

[github-actions-shield]: https://github.com/rayokota/hentitydb/workflows/build/badge.svg
[github-actions-link]: https://github.com/rayokota/hentitydb/actions
[maven-shield]: https://img.shields.io/maven-central/v/io.hentitydb/hentitydb.svg
[maven-link]: https://search.maven.org/#search%7Cga%7C1%7Cio.hentitydb
[javadoc-shield]: https://javadoc.io/badge/io.hentitydb/hentitydb.svg?color=blue
[javadoc-link]: https://javadoc.io/doc/io.hentitydb/hentitydb

HEntityDB is a client layer for using HBase as an entity database.  It supports the Entity and Sorted Collection application archetypes as described [here](https://yokota.blog/2017/07/12/hbase-application-archetypes-redux/).

## Installing

Releases of HEntityDB are deployed to Maven Central.

```xml
<dependency>
    <groupId>io.hentitydb</groupId>
    <artifactId>hentitydb</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Building

You can also choose to build HEntityDB manually.  Prerequisites for building:

* git
* Maven
* Java 8

```
git clone https://github.com/rayokota/hentitydb.git
cd hentitydb
mvn clean package -DskipTests
```

## Deployment

HEntityDB makes use of server-side filters.  To deploy HEntityDB:

* Add target/hentitydb-1.0.0-deployment.jar to the classpath of all HBase region servers.
* Restart the HBase region servers.
    
## Defining an Entity

First, your class needs to be annotated as shown below.  

```java
@Entity
@Table("users")
public class User {
    @Id
    private long id;
    
    @Column
    private String network;
    
    @Column
    private String name;
    
    @Column
    @Enumerated(EnumType.ORDINAL)
    private UserType userType;

    public User() {
    }

    public User(long id, String network, String name, UserType userType) {
        this.id = id;
        this.network = network;
        this.name = name;
        this.userType = userType;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getNetwork() {
        return network;
    }
    
    public void setNetwork(String network) {
        this.network = network;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    
    public UserType getUserType() {
        return userType;
    }

    public void setUserType(UserType userType) {
        this.userType = userType;
    }
}
```

As you can see above, the `@Id` annotation indicates the field that will hold the row ID (or row key).  The row ID can also be marked with a `@Salt` annotation, to prevent hotspotting for monotonically increasing IDs.  Additional fields that should be persisted are marked with a `@Column` annotation.

For annotated fields, the following types are supported:

- String
- Byte or byte
- Short or short
- Integer or int
- Long or long
- Float or float
- Double or double
- BigDecimal
- Boolean or boolean
- byte[]
- Date
- UUID
- Enum

Enums must be marked with `@Enumerated(EnumType.ORDINAL)` or `@Enumerated(EnumType.STRING)`.  If the field is not one of the above types, a custom codec can be specified using a `@Codec` annotation:

```java
@Entity
@Table("messages")
public class Message {
    @Id
    private long userId;
    
    @ElementId
    @Column
    private long messageId;
    
    @Column
    private long senderId;

    @Column
    private long createdAt;
    
    @Column
    @Codec(VarIntArrayCodec.class)
    private int[] myInts;
    
    ...
}
```

Each entity can have a different TTL, by specifying a method with a `@TTL` annotation:

    
```java 
@Entity
@Table("messages")
public class Message {
    @Id
    private long userId;
    
    ...
    
    @TTL
    public Integer getTTL() {
        // custom logic to calculate a TTL, in milliseconds
    }
}
```

## Defining an Entity Collection

Since multiple operations on the same row can be performed atomically in HBase, it is often desirable to store more than one entity in a row.  This is achieved by specifying an additional set of *element IDs*.  For example, to store all messages for a user in the same row, a message could be modelled as follows:

```java
@Entity
@Table("messages")
public class Message {
    @Id
    private long userId;
    
    @ElementId
    @Column
    private long messageId;
    
    @Column
    private long senderId;

    @Column
    private long createdAt;
    
    ...
}
```

For the above example, the user ID is the collection ID, while the message ID is the element ID.  Multiple `@ElementId` annotations can be specified if the element key is a composite.

Elements of a collection can also be saved in multiple column families, by using the `@ColumnFamilies` annotation:

```java
@Entity
@Table("messages")
@ColumnFamilies({
    @ColumnFamily(name = "readMessages"), 
    @ColumnFamily(name = "unreadMessages")
})
public class Message {

    @ColumnFamilyName
    private String family;
    
    @Id
    private long userId;
    
    ...
}
```
    
In the above example, we also specify an additional field to hold the actual name of the column family for an entity instance, using `@ColumnFamilyName`.

## Setup

To use HEntityDB, you must first obtain an entity context.  The entity context is parameterized with the type of the entity and the type of the entity ID.

```java
Configuration hconfig = HBaseConfiguration.create();
EntityConfiguration config = new EntityConfiguration(hconfig);
config.setAutoTableCreation(true);
ConnectionFactory factory = Environment.getConnectionFactory(config);
Connection conn = factory.createConnection();
EntityContext<User, Long> context = Environment.getEntityContext(conn, User.class);
```

## Querying Entities

To retrieve all messages for a given user:

```java
List<Message> messages = context.get(userId);
```
    
In addition, a select query can be used to retrieve entities.  For example, to get all messages for a given user that have a message ID between 100 and 199:

```java
List<Message> messages = context.select().whereId().eq(userId)
    .whereElementId("messageId").gte(100)
    .whereElementId("messageId").lte(199)
    .fetch();
```
    	
Or to get the first 1000 messages for a given user that were sent by user 1234:

```java
List<Message> messages = context.select().whereId().eq(userId)
    .whereColumn("senderId").eq(1234)
    .limit(1000)
    .fetch();
```
    	
## Creating, Updating, and Deleting Entities

Entities can be created as follows:

```java
context.put(myMessage);
```
    

To modify an entity, use an update query:

```java
context.update().whereId().eq(userId)
    .whereElementId("messageId").eq(messageId)
    .setColumn("senderId", 4321)
    .execute();
```
      
To atomically modify an entity, based on some column matching an expected value, use an if-check:

```java
context.update().whereId().eq(userId)
    .whereElementId("messageId").eq(messageId)
    .setColumn("senderId", 4321)
    .ifElementId("messageId").eq(messageId)
    .ifColumn("senderId", 1234)
    .execute();
```

In the above example, the sender ID will only be modified if the current value is 1234.  In this case, the if-check refers to the same column that is being modified, but that does not always have to be the case -- it can refer to any other column in the same row.

To delete a specific entity, use the following:

```java
context.remove(myMessage);
```
    
Or use a delete query:

```java
context.delete().whereId().eq(userId)
    .whereElementId("messageId").eq(messageId)
    .execute();
```
        
Or to delete all entities for a given row ID:

```java
context.delete(userId);
```
    
## Atomic Groups of Entity Mutations

To perform multiple entity mutations atomically on a single row, use a mutations query:

```java
UpdateQuery<Message, Long> update = context.update()
    .whereId().eq(userId)
    .whereElementId("messageId").eq(messageId1)
    .setColumn("senderId", 4321);
    
DeleteQuery<Message, Long> delete = context.delete()
    .whereId().eq(userId)
    .whereElementId("messageId").eq(messageId2);
    
context.mutate().whereId().eq(userId)
    .add(update, delete)
    .execute();
```
        
Note that the row IDs of all the above queries must match.  Also, if-checks can be added to a mutations query as well.


