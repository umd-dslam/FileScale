DROP TABLE IF EXISTS inodes;
CREATE TABLE inodes(
    id int primary key, parent int, name text,
    accessTime bigint, modificationTime bigint,
    header bigint, permission bigint, blockIds bigint[]
);

DROP TABLE IF EXISTS datablocks;
CREATE TABLE datablocks(
    blockId bigint primary key, numBytes bigint, generationStamp bigint,
    eplication int, bcId bigint, storageID text[]
);

DROP TABLE IF EXISTS datanodeStorageInfo;
CREATE TABLE datanodeStorageInfo(
    storageID text primary key, storageType int, State int,
    capacity bigint, dfsUsed bigint, nonDfsUsed bigint, remaining bigint,
    blockPoolUsed bigint, blockReportCount int, heartbeatedSinceFailover boolean,
    blockContentsStale boolean, datanodeUuid text
);

DROP TABLE IF EXISTS datanodeDescriptor;
CREATE TABLE datanodeDescriptor(
    datanodeUuid text primary key, datanodeUuidBytes bytea, ipAddr text,
    ipAddrBytes bytea, hostName text, hostNameBytes bytea, peerHostName text,
    xferPort int, infoPort int, infoSecurePort int, ipcPort int, xferAddr text, 
    capacity bigint, dfsUsed bigint, nonDfsUsed bigint, remaining bigint,
    blockPoolUsed bigint, cacheCapacity bigint, cacheUsed bigint, lastUpdate bigint, 
    lastUpdateMonotonic bigint, xceiverCount int, location text, softwareVersion text,
    dependentHostNames text[], upgradeDomain text, numBlocks int, adminState text,
    maintenanceExpireTimeInMS bigint, lastBlockReportTime bigint, lastBlockReportMonotonic bigint,
    lastCachingDirectiveSentTimeMs bigint, isAlive boolean, needKeyUpdate boolean,
    forceRegistration boolean, bandwidth bigint, lastBlocksScheduledRollTime bigint,
    disallowed boolean, pendingReplicationWithoutTargets int, heartbeatedSinceRegistration boolean 
);

DROP TABLE IF EXISTS volumeFailureSummary;
CREATE TABLE volumeFailureSummary(
    datanodeUuid text primary key, failedStorageLocations text[],
    volumeFailures bigint, estimatedCapacityLostTotal bigint
);
