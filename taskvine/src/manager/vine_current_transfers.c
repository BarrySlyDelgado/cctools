/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_current_transfers.h"
#include "macros.h"
#include "vine_file_replica.h"
#include "vine_file_replica_table.h"
#include "vine_blocklist.h"
#include "vine_manager.h"
#include "xxmalloc.h"

#include "debug.h"

struct vine_transfer_pair {
	struct vine_worker_info *dest_worker;
	struct vine_worker_info *source_worker;
	char *source_url;
};

static struct vine_transfer_pair *vine_transfer_pair_create(struct vine_worker_info *dest_worker, struct vine_worker_info *source_worker, const char *source_url)
{
	struct vine_transfer_pair *t = malloc(sizeof(struct vine_transfer_pair));
	t->dest_worker = dest_worker;
	t->source_worker = source_worker;
	t->source_url = source_url ? xxstrdup(source_url) : 0;

	if (t->dest_worker) {
		t->dest_worker->incoming_xfer_counter++;
	}
	if (t->source_worker) {
		t->source_worker->outgoing_xfer_counter++;
	}

	return t;
}

static void vine_transfer_pair_delete(struct vine_transfer_pair *p)
{
	if (p) {
		if (p->dest_worker) {
			p->dest_worker->incoming_xfer_counter--;
		}
		if (p->source_worker) {
			p->source_worker->outgoing_xfer_counter--;
		}
		free(p->source_url);
		free(p);
	}
}

// add a current transaction to the transfer table
char *vine_current_transfers_add(struct vine_manager *q, struct vine_worker_info *dest_worker, struct vine_worker_info *source_worker, const char *source_url)
{
	cctools_uuid_t uuid;
	cctools_uuid_create(&uuid);

	char *transfer_id = strdup(uuid.str);
	struct vine_transfer_pair *t = vine_transfer_pair_create(dest_worker, source_worker, source_url);

	hash_table_insert(q->current_transfer_table, transfer_id, t);
	return transfer_id;
}

// remove a completed transaction from the transfer table - i.e. open the source to an additional transfer
int vine_current_transfers_remove(struct vine_manager *q, const char *id)
{
	struct vine_transfer_pair *p;
	p = hash_table_remove(q->current_transfer_table, id);
	if (p) {
		vine_transfer_pair_delete(p);
		return 1;
	} else {
		return 0;
	}
}

void set_throttle(struct vine_manager *m, struct vine_worker_info *w, int is_destination)
{
	if (!w) {
		return;
	}

	int good;
	int bad;
	int streak;

	int grace = 5; // XXX: make tunable parameter: q->consecutieve_max_xfer_errors;
	const char *dir;

	if (is_destination) {
		good = w->xfer_total_good_destination_counter;
		bad = w->xfer_total_bad_destination_counter;
		streak = w->xfer_streak_bad_destination_counter;
		dir = "destination";
		// since worker can talk to manager, probably the issue is with sources. Give more chances to
		// destinations.
		grace *= 2;
	} else {
		good = w->xfer_total_good_source_counter;
		bad = w->xfer_total_bad_source_counter;
		streak = w->xfer_streak_bad_source_counter;
		dir = "source";
	}

	debug(D_VINE, "Setting transfer failure (%d,%d/%d) timestamp on %s worker: %s:%d", streak, bad, good + bad, dir, w->hostname, w->transfer_port);

	w->last_transfer_failure = timestamp_get();

	/* first error treat as a normal error */
	if (streak < grace) {
		return;
	}

	if (good <= bad) {
		/* this worker has failed more often than not, release it. */
		notice(D_VINE, "Releasing worker %s because of repeated %s transfer failures: %d/%d", dir, w->addrport, bad, bad + good);
		vine_manager_remove_worker(m, w, VINE_WORKER_DISCONNECT_XFER_ERRORS);
	}
}

int vine_current_transfers_set_failure(struct vine_manager *q, char *id, const char *cachename)
{
	struct vine_transfer_pair *p = hash_table_lookup(q->current_transfer_table, id);

	/* If there is no matching transfer record, then it means a worker failed and the record was removed b/c of wipe_worker. */
	if (!p) {
		return 0;
	}

	struct vine_worker_info *source_worker = p->source_worker;
	struct vine_worker_info *dest_worker = p->dest_worker;

	/* If p is valid, the elements of p should always be valid, because a failed worker causes the transfer record to be removed,
	 * not nulled out. This shouldn't happen, but we check and emit an error just in case. */
	if (!source_worker || !dest_worker) {
		if (!source_worker) {
			debug(D_ERROR, "%s: transfer record for file %s with id %s is found, but source worker is null", __func__, cachename, id);
		}
		if (!dest_worker) {
			debug(D_ERROR, "%s: transfer record for file %s with id %s is found, but destination worker is null", __func__, cachename, id);
		}
		return 0;
	}

	/* The transfer is considered a worker fault only if the replica exists on the source worker
	 * and its state is VINE_FILE_REPLICA_STATE_READY, meaning the source has the replica but, for
	 * some reason, failed to transfer it to the destination. */
	struct vine_file_replica *source_replica = vine_file_replica_table_lookup(source_worker, cachename);
	if (source_replica && source_replica->state == VINE_FILE_REPLICA_STATE_READY) {
		debug(D_VINE, "Unable to transfer a READY replica from %s (%s) to %s (%s) for file %s \n", source_worker->hostname, source_worker->addrport, dest_worker->hostname, dest_worker->addrport, cachename);

		source_worker->xfer_streak_bad_source_counter++;
		source_worker->xfer_total_bad_source_counter++;
		set_throttle(q, source_worker, 0);

		dest_worker->xfer_streak_bad_destination_counter++;
		dest_worker->xfer_total_bad_destination_counter++;
		set_throttle(q, dest_worker, 1);
		return 1;
	}

	return 0;
}

void vine_current_transfers_set_success(struct vine_manager *q, char *id)
{
	struct vine_transfer_pair *p = hash_table_lookup(q->current_transfer_table, id);

	if (!p) {
		return;
	}

	struct vine_worker_info *source = p->source_worker;
	if (source) {
		vine_blocklist_unblock(q, source->addrport);

		source->xfer_streak_bad_source_counter = 0;
		source->xfer_total_good_source_counter++;
	}

	struct vine_worker_info *dest_worker = p->dest_worker;
	if (dest_worker) {
		vine_blocklist_unblock(q, dest_worker->addrport);

		dest_worker->xfer_streak_bad_destination_counter = 0;
		dest_worker->xfer_total_good_destination_counter++;
	}
}

// count the number transfers coming from a specific remote url (not a worker)
int vine_current_transfers_url_in_use(struct vine_manager *q, const char *source)
{
	char *id;
	struct vine_transfer_pair *t;
	int c = 0;
	HASH_TABLE_ITERATE(q->current_transfer_table, id, t)
	{
		if (source == t->source_url)
			c++;
	}
	return c;
}

// remove all transactions involving a worker from the transfer table - if a worker failed or is being deleted
// intentionally
int vine_current_transfers_wipe_worker(struct vine_manager *q, struct vine_worker_info *w)
{
	debug(D_VINE, "Removing instances of worker from transfer table");

	int removed = 0;
	if (!w) {
		return removed;
	}

	struct list *ids_to_remove = list_create();

	char *id;
	struct vine_transfer_pair *t;
	HASH_TABLE_ITERATE(q->current_transfer_table, id, t)
	{
		if (t->dest_worker == w || t->source_worker == w) {
			list_push_tail(ids_to_remove, xxstrdup(id));
		}
	}

	list_first_item(ids_to_remove);
	char *transfer_id;
	while ((transfer_id = list_pop_head(ids_to_remove))) {
		vine_current_transfers_remove(q, transfer_id);
		free(transfer_id);
		removed++;
	}

	list_delete(ids_to_remove);

	return removed;
}

void vine_current_transfers_print_table(struct vine_manager *q)
{
	char *id;
	struct vine_transfer_pair *t;
	struct vine_worker_info *w;
	debug(D_VINE, "-----------------TRANSFER-TABLE--------------------");
	HASH_TABLE_ITERATE(q->current_transfer_table, id, t)
	{
		w = t->source_worker;
		if (w) {
			debug(D_VINE, "%s : source: %s:%d url: %s", id, w->transfer_host, w->transfer_port, t->source_url);
		} else {
			debug(D_VINE, "%s : source: remote url: %s", id, t->source_url);
		}
	}
	debug(D_VINE, "-----------------END-------------------------------");
}

void vine_current_transfers_clear(struct vine_manager *q)
{
	hash_table_clear(q->current_transfer_table, (void *)vine_transfer_pair_delete);
}

int vine_current_transfers_get_table_size(struct vine_manager *q)
{
	return hash_table_size(q->current_transfer_table);
}
