- LogeType
    - Per-type metadata
        - TypeName
        - Version (schema)
        - Memory settings (transient, permanent, LRU)
        - Etc

- LogeObject
    - Per-object metadata
        - Key 
        - Object version
        - Dirty flag
        - Transaction refcount

    - Scaffolding
        - Pointer to LogeType
        - Pointer to live object

- LogeTransaction
    - Transaction context
    - All updates go through this

