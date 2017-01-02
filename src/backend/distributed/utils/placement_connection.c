/*-------------------------------------------------------------------------
 *
 * placement_connection.c
 *   Per-Placement connection & transaction handling
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "distributed/connection_management.h"
#include "distributed/placement_connection.h"
#include "distributed/metadata_cache.h"
#include "distributed/hash_helpers.h"
#include "utils/hsearch.h"

/*
 * Hash table mapping placements to a list of connections.
 *
 * This stores a list of connections for each placement, because multiple
 * connections to the same placement may exist at the same time. E.g. a
 * real-time executor query may reference the same placement in several
 * sub-tasks.
 *
 * We keep track about a connection having executed DML or DDL, since we can
 * only ever allow a single transaction to do either to prevent deadlocks and
 * consistency violations (e.g. read-your-own-writes).
 */

/* information about a connection reference to a placement */
typedef struct ConnectionReference
{
	uint64 shardId;
	uint64 placementId;

	MultiConnection *connection;

	bool hadDML;
	bool hadDDL;

	/* membership in ConnectionPlacementHashKey->connectionReferences */
	dlist_node placementNode;

	/* membership in MultiConnection-> */
	dlist_node connectionNode;
} ConnectionReference;

/* hash key */
typedef struct ConnectionPlacementHashKey
{
	uint64 placementId;
} ConnectionPlacementHashKey;

/* hash entry */
typedef struct ConnectionPlacementHashEntry
{
	ConnectionPlacementHashKey key;

	/* did any remote transactions fail? */
	bool failed;

	/* list of connections to remote nodes */
	dlist_head connectionReferences;

	/* membership in ConnectionShardHashEntry->placementConnections */
	dlist_node shardNode;
} ConnectionPlacementHashEntry;

/* hash table */
static HTAB *ConnectionPlacementHash;


/*
 * Hash table mapping shard ids to placements.
 *
 * This is used to track whether placements of a shard have to be marked
 * invalid after a failure, or whether a coordinated transaction has to be
 * aborted, to avoid all placements of a shard to be marked invalid.
 */

/* hash key */
typedef struct ConnectionShardHashKey
{
	uint64 shardId;
} ConnectionShardHashKey;

/* hash entry */
typedef struct ConnectionShardHashEntry
{
	ConnectionShardHashKey key;
	dlist_head placementConnections;
} ConnectionShardHashEntry;

/* hash table itself */
static HTAB *ConnectionShardHash;


static void AssociatePlacementWithShard(ConnectionPlacementHashEntry *placementEntry,
										ShardPlacement *placement);
static bool CheckShardPlacements(ConnectionShardHashEntry *shardEntry, bool preCommit,
								 bool using2PC);


/*
 * GetPlacementConnection establishes a connection for a placement.
 *
 * See StartPlacementConnection for details.
 */
MultiConnection *
GetPlacementConnection(uint32 flags, ShardPlacement *placement)
{
	MultiConnection *connection = StartPlacementConnection(flags, placement);

	FinishConnectionEstablishment(connection);
	return connection;
}


/*
 * StartPlacementConnection() initiates a connection to a remote node,
 * associated with the placement and transaction.
 *
 * The connection is established as the current user & database.
 *
 * See StartNodeUserDatabaseConnection for details.
 *
 * Flags have the corresponding meaning from StartNodeUserDatabaseConnection,
 * except that two additional flags have an effect:
 * - FOR_DML - signal that connection is going to be used for DML (modifications)
 * - FOR_DDL - signal that connection is going to be used for DDL
 *
 * Only one connection associated with the placement may have FOR_DML or
 * FOR_DDL set. This restriction prevents deadlocks and wrong results due to
 * in-progress transactions.
 */
MultiConnection *
StartPlacementConnection(uint32 flags, ShardPlacement *placement)
{
	ConnectionPlacementHashKey key;
	ConnectionPlacementHashEntry *placementEntry = NULL;
	bool found = false;
	ConnectionReference *returnConnectionReference = NULL;
	dlist_iter it;

	key.placementId = placement->placementId;

	/* lookup relevant hash entry */
	placementEntry = hash_search(ConnectionPlacementHash, &key, HASH_ENTER, &found);
	if (!found)
	{
		dlist_init(&placementEntry->connectionReferences);
		placementEntry->failed = false;
	}


	/*
	 * Check whether any of the connections already associated with the
	 * placement can be reused, or violates FOR_DML/FOR_DDL constraints.
	 */
	dlist_foreach(it, &placementEntry->connectionReferences)
	{
		ConnectionReference *connectionReference =
			dlist_container(ConnectionReference, placementNode, it.cur);
		MultiConnection *connection = connectionReference->connection;
		bool useConnection = false;

		/* use the connection, unless in a state that's not useful for us */
		if (!connection ||
			connection->claimedExclusively ||
			(flags & FORCE_NEW_CONNECTION) != 0 ||
			returnConnectionReference != NULL)
		{
			useConnection = false;
		}
		else
		{
			useConnection = true;
		}

		/*
		 * If not using the connection, verify that FOR_DML/DDL flags are
		 * compatible.
		 */
		if (useConnection)
		{
			returnConnectionReference = connectionReference;
		}
		else if (connectionReference->hadDDL)
		{
			/* XXX: errcode & errmsg */
			ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
							errmsg("cannot establish new placement connection when other "
								   "placement executed DDL")));
		}
		else if (connectionReference->hadDML)
		{
			/* XXX: errcode & errmsg */
			ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
							errmsg("cannot establish new placement connection when other "
								   "placement executed DML")));
		}
	}

	/*
	 * Either no caching desired, or no connection present. Start connection
	 * establishment.
	 */
	if (returnConnectionReference == NULL)
	{
		MultiConnection *connection = StartNodeConnection(flags, placement->nodeName,
														  placement->nodePort);

		/* FIXME: use xact context ? */
		returnConnectionReference = (ConnectionReference *)
									MemoryContextAlloc(ConnectionContext,
													   sizeof(ConnectionReference));
		returnConnectionReference->connection = connection;
		returnConnectionReference->hadDDL = false;
		returnConnectionReference->hadDML = false;
		returnConnectionReference->shardId = placement->shardId;
		returnConnectionReference->placementId = placement->placementId;
		dlist_push_tail(&placementEntry->connectionReferences,
						&returnConnectionReference->placementNode);

		/* record association with shard, for invalidation */
		AssociatePlacementWithShard(placementEntry, placement);

		/* record association with connection, to handle connection closure */
		dlist_push_tail(&connection->referencedPlacements,
						&returnConnectionReference->connectionNode);
	}

	if (flags & FOR_DDL)
	{
		returnConnectionReference->hadDDL = true;
	}
	if (flags & FOR_DML)
	{
		returnConnectionReference->hadDML = true;
	}

	return returnConnectionReference->connection;
}


/* Record shard->placement relation */
static void
AssociatePlacementWithShard(ConnectionPlacementHashEntry *placementEntry,
							ShardPlacement *placement)
{
	ConnectionShardHashKey shardKey;
	ConnectionShardHashEntry *shardEntry = NULL;
	bool found = false;
	dlist_iter placementIter;

	shardKey.shardId = placement->shardId;
	shardEntry = hash_search(ConnectionShardHash, &shardKey, HASH_ENTER, &found);
	if (!found)
	{
		dlist_init(&shardEntry->placementConnections);
	}

	/*
	 * Check if placement is already associated with shard (happens if there's
	 * multiple connections for a placement).  There'll usually only be few
	 * placement per shard, so the price of iterating isn't large.
	 */
	dlist_foreach(placementIter, &shardEntry->placementConnections)
	{
		ConnectionPlacementHashEntry *placementEntry =
			dlist_container(ConnectionPlacementHashEntry, shardNode, placementIter.cur);

		if (placementEntry->key.placementId == placement->placementId)
		{
			return;
		}
	}

	/* otherwise add */
	dlist_push_tail(&shardEntry->placementConnections, &placementEntry->shardNode);
}


void
CloseShardPlacementAssociation(struct MultiConnection *connection)
{
	dlist_iter placementIter;

	dlist_foreach(placementIter, &connection->referencedPlacements)
	{
		ConnectionReference *reference =
			dlist_container(ConnectionReference, connectionNode, placementIter.cur);

		reference->connection = NULL;
	}
}


void
ResetShardPlacementAssociation(struct MultiConnection *connection)
{
	dlist_init(&connection->referencedPlacements);
}


void
InitPlacementConnectionManagement(void)
{
	HASHCTL info;
	uint32 hashFlags = 0;

	/* create (placementId) -> [connection] hash */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(ConnectionPlacementHashKey);
	info.entrysize = sizeof(ConnectionPlacementHashEntry);
	info.hash = tag_hash;
	info.hcxt = ConnectionContext;
	hashFlags = (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	ConnectionPlacementHash = hash_create("citus connection cache (placementid)",
										  64, &info, hashFlags);

	/* create (shardId) -> [ConnectionShardHashEntry] hash */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(ConnectionShardHashKey);
	info.entrysize = sizeof(ConnectionShardHashEntry);
	info.hash = tag_hash;
	info.hcxt = ConnectionContext;
	hashFlags = (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	ConnectionShardHash = hash_create("citus connection cache (shardid)",
									  64, &info, hashFlags);
}


/*
 * Disassociate connections from placements and shards. This will be called at
 * the end of XACT_EVENT_COMMIT and XACT_EVENT_ABORT.
 */
void
ResetPlacementConnectionManagement(void)
{
	/* Simply delete all entries*/
	hash_delete_all(ConnectionPlacementHash);
	hash_delete_all(ConnectionShardHash);

	/* FIXME: this is leaking memory for the ConnectionReference structs */
}


/*
 * Check which placements have to be marked as invalid, and/or whether
 * sufficiently many placements have failed to abort the entire coordinated
 * transaction.
 *
 * This will usually be called twice. Once before the remote commit is done,
 * and once after. This is so we can abort before executing remote commits,
 * and so we can handle remote transactions that failed during commit.
 *
 * When preCommit or using2PC is true, failures on transactions marked as
 * critical will abort the entire coordinated transaction. Otherwise we can't
 * anymore, because some remote transactions might have already committed.
 */
void
CheckForFailedPlacements(bool preCommit, bool using2PC)
{
	HASH_SEQ_STATUS status;
	ConnectionShardHashEntry *shardEntry = NULL;
	int successes = 0;
	int attempts = 0;
	hash_seq_init(&status, ConnectionShardHash);
	while ((shardEntry = (ConnectionShardHashEntry *) hash_seq_search(&status)) != 0)
	{
		attempts++;
		if (CheckShardPlacements(shardEntry, preCommit, using2PC))
		{
			successes++;
		}
	}

	/*
	 * If no shards could be modified at all, error out. Doesn't matter if
	 * we're post-commit - there's nothing to invalidate.
	 */
	if (attempts > 0 && successes == 0)
	{
		ereport(ERROR, (errmsg("could not commit transaction on any active node")));
	}
}


static bool
CheckShardPlacements(ConnectionShardHashEntry *shardEntry,
					 bool preCommit, bool using2PC)
{
	int failures = 0;
	int successes = 0;
	dlist_iter placementIter;

	dlist_foreach(placementIter, &shardEntry->placementConnections)
	{
		ConnectionPlacementHashEntry *placementEntry =
			dlist_container(ConnectionPlacementHashEntry, shardNode, placementIter.cur);
		dlist_iter referenceIter;

		dlist_foreach(referenceIter, &placementEntry->connectionReferences)
		{
			ConnectionReference *reference =
				dlist_container(ConnectionReference, placementNode, referenceIter.cur);
			MultiConnection *connection = reference->connection;

			/*
			 * If neither DDL nor DML were executed, there's no need for
			 * invalidation.
			 */
			if (!reference->hadDDL && !reference->hadDML)
			{
				continue;
			}

			/*
			 * Failed if connection was closed, or remote transaction was
			 * unsuccessful.
			 */
			if (!connection || connection->remoteTransaction.transactionFailed)
			{
				placementEntry->failed = true;
			}
		}

		if (placementEntry->failed)
		{
			failures++;
		}
		else
		{
			successes++;
		}
	}

	if (failures > 0 && successes == 0)
	{
		int elevel = 0;

		/*
		 * Only error out if we're pre-commit or using 2PC. Otherwise we can
		 * end up with a state where parts of the transaction is committed and
		 * others aren't, without correspondingly marking things as invalid
		 * (which we can't, as we would have already committed).  This sucks.
		 */
		if (preCommit || using2PC)
		{
			elevel = ERROR;
		}
		else
		{
			elevel = WARNING;
		}

		ereport(elevel,
				(errmsg("could not commit transaction for shard "INT64_FORMAT
						" on any active node",
						shardEntry->key.shardId)));
		return false;
	}

	/* mark all failed placements invalid */
	dlist_foreach(placementIter, &shardEntry->placementConnections)
	{
		ConnectionPlacementHashEntry *placementEntry =
			dlist_container(ConnectionPlacementHashEntry, shardNode, placementIter.cur);

		if (placementEntry->failed)
		{
			UpdateShardPlacementState(placementEntry->key.placementId, FILE_INACTIVE);
		}
	}

	return true;
}
