
- HDFS FS directory tree and data block indexes are persisted in FSImage.
- The relation between data blocks and datanode haven't been stored in FSImage. In contrast, it's dynamically constructed from the heartbelt of datanode.

```java
class Block {
    long blockId;
    long numBytes;
    long generationStamp;

    get/set();
    marshal/unmarshal();
    ...
}

class BlockInfo extends Block {
private short replication;
private volatile long bcId;
DatanodeStorageInfo[] storages;
BlockUnderConstructionFeature uc;
}
```

```bash
## map: file <-> data blocks
| InodeId | ... | {blockId} |
```

http://www.postgresqltutorial.com/postgresql-array/
https://blog.2ndquadrant.com/using-java-arrays-to-insert-retrieve-update-postgresql-arrays/


```bash
## map: data blocks <-> data node

table block + blockInfo
----------------
| blockId | numBytes | generationStamp | replication | bcId | {storageID}

table DatanodeStorageInfo
----------------
storageID | storageType | State | capacity | dfsUsed | nonDfsUsed | remaining | blockPoolUsed | blockReportCount | heartbeatedSinceFailover | blockContentsStale | FoldedTreeSet<BlockInfo> blocks | datanodeUuid |

table DatanodeDescriptor
----------------
| ipAddr | ipAddrBytes | hostName | hostNameBytes | peerHostName | xferPort | infoPort | infoSecurePort | ipcPort | xferAddr  | datanodeUuid | datanodeUuidBytes | capacity | dfsUsed |  nonDfsUsed | remaining | blockPoolUsed | cacheCapacity| cacheUsed | lastUpdate | lastUpdateMonotonic | xceiverCount | location | softwareVersion | List<String> dependentHostNames | upgradeDomain | numBlocks | adminState | maintenanceExpireTimeInMS | lastBlockReportTime | lastBlockReportMonotonic | lastCachingDirectiveSentTimeMs | isAlive | needKeyUpdate | forceRegistration | bandwidth | lastBlocksScheduledRollTime | disallowed | pendingReplicationWithoutTargets | heartbeatedSinceRegistration

EnumCounters??
CachedBlocksList ??


table VolumeFailureSummary
-----------------
| datanodeUuid | String[] failedStorageLocations | volumeFailures | lastVolumeFailureDate | estimatedCapacityLostTotal
```


```java
 // page: 180
 class BlockManager {
     BlocksMap;
     final CorruptReplicasMap corruptReplicas = new CorruptReplicasMap();
     private final InvalidateBlocks InvalidateBlocks;
     UnderReplicatedBlocks;
     PendingReplicationBlocks;

 }

```

```bash
## page 195
## add data block
FSNamesystem.getAdditionalBlock() -> FSDirectory.addBlock() -> BlockManager.addBlockCollection() -> blocksMap.addBlockCollection()
                                                            -> INodeFile.addBlock() -> blocks

## add replicas
DatanodeProtocol.blockReport() or DatanodeProtocol.blockReceivedAndDeleted() -> Namenode -> BlockManager.addStoredBlock() -> storagelnfo.addBlock() -> BlockManager.blocksMap -> UC to COMMITTED

## delete data block

FSNamesystem.delete() -> deleteInt() -> deleteIntenal() -> FSDirectory.delete() -> collectedBlocks or removedINodes -> removePathAndBlocks() -> removeBlocks() -> Block.Manager.removeBlock() ->
blocksMap, postponedMisreplicatedBlocksCount, pendingReplications, neededReplications, corruptReplicas
-> addToUbvalidates()

## delete replicas

1. delete file 2. too many relicas 3. corrupt replicas

## copy data block
1. write file -> not enough replicas -> neededReplications
2. delete datanode -> copy all replicas in datanode into neededReplications
3. pendingReplications timeout -> back to neededReplications 
```

```bash
## page 213
BlockManager.processReport() -> processFirstBlockReport() -> addStoredBlocklmmediate() -> DatanodeStoragelnfo.addBlock()
                                                          -> markBlockAsCorrupt()

### page 222
DatanodeDescriptor

### 224
DatanodeStoragelnfo

### 
DatanodeManager

### 243
LeaseManager

### 258
CacheManager 
```

