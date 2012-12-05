- Schemas
    - Primitive types: int, float, etc, embedded in object
    - Variable types: string, binary, stored externally
        - Hash of value embedded in object
        - Avoid rewriting large values when possible

    - Reference types:
        - List
        - Map, string keys
  

- Storage

    - Types of stored entity:
        - Object (fixed size)
        - External data (string, variable binary, array-of-obj-IDs)
        
    - Initially:
        - One file per entity

    - Later:
        - One file per object type, mmapped
        - Collection of files for externals

    - Fsyncs
        - Immediately?
        - Never?
        - Periodic with queue of dirty FDs?


- Versioning
    - Everything versioned, immutable
    - In-memory store is map of objID -> linked list, head is current version

  
- Transactions
    - Transaction context keeps (new obj, prev version) list for modified
        (and, optionally, read) objects
    - At commit time, lock tracked objects, check versions, write, release
    - If any version doesn't match, fail the transaction


- Replication
    - One-way
    - Streaming log
    - Embed checkpoints after transaction writes
    - Apply stream on slave only at checkpoints
    - Some kind of global versioning for switching masters?
        - Timestamp on checkpoints
        - Write log includes previous version of each object
        - Last transaction received is new canonical state
        - When old master comes back up, ask new master for most recent
          timestamp it applied, roll back own data from stored write log
          to previous versions


- Sharding
    - Not yet, but based on circular hash of object type / PK


  
