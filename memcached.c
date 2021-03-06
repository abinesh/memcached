/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *  memcached - memory caching daemon
 *
 *       http://www.danga.com/memcached/
 *
 *  Copyright 2003 Danga Interactive, Inc.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      Anatoly Vorobey <mellon@pobox.com>
 *      Brad Fitzpatrick <brad@danga.com>
 */
#include "memcached.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <ctype.h>
#include <stdarg.h>
#include <math.h>


/* some POSIX systems need the following definition
 * to get mlockall flags out of sys/mman.h.  */
#ifndef _P1003_1B_VISIBLE
#define _P1003_1B_VISIBLE
#endif
/* need this to get IOV_MAX on some platforms. */
#ifndef __need_IOV_MAX
#define __need_IOV_MAX
#endif
#include <pwd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <limits.h>
#include <sysexits.h>
#include <stddef.h>

/* FreeBSD 4.x doesn't have IOV_MAX exposed. */
#ifndef IOV_MAX
#if defined(__FreeBSD__) || defined(__APPLE__)
# define IOV_MAX 1024
#endif
#endif

/*
 * forward declarations
 */
static void drive_machine(conn *c);
static int new_socket(struct addrinfo *ai);
static int try_read_command(conn *c);

enum try_read_result {
    READ_DATA_RECEIVED,
    READ_NO_DATA_RECEIVED,
    READ_ERROR,            /** an error occured (on the socket) (or client closed connection) */
    READ_MEMORY_ERROR      /** failed to allocate more memory */
};

static enum try_read_result try_read_network(conn *c);
static enum try_read_result try_read_udp(conn *c);

static void conn_set_state(conn *c, enum conn_states state);

/* stats */
static void stats_init(void);
static void server_stats(ADD_STAT add_stats, conn *c);
static void process_stat_settings(ADD_STAT add_stats, void *c);


/* defaults */
static void settings_init(void);

/* event handling, network IO */
static void event_handler(const int fd, const short which, void *arg);
static void conn_close(conn *c);
static void conn_init(void);
static bool update_event(conn *c, const int new_flags);
static void complete_nread(conn *c);
static void process_command(conn *c, char *command);
static void write_and_free(conn *c, char *buf, int bytes);
static int ensure_iov_space(conn *c);
static int add_iov(conn *c, const void *buf, int len);
static int add_msghdr(conn *c);


static void conn_free(conn *c);

/** exported globals **/
struct stats stats;
struct settings settings;
time_t process_started;     /* when the process was started */

struct slab_rebalance slab_rebal;
volatile int slab_rebalance_signal;

/** file scope variables **/
static conn *listen_conn = NULL;
static struct event_base *main_base;

enum transmit_result {
    TRANSMIT_COMPLETE,   /** All done writing. */
    TRANSMIT_INCOMPLETE, /** More data remaining to write. */
    TRANSMIT_SOFT_ERROR, /** Can't write any more right now. */
    TRANSMIT_HARD_ERROR  /** Can't write (c->state is set to conn_closing) */
};

static enum transmit_result transmit(conn *c);

//IP address and port number of join node
static char join_server_port_number[255];
static char join_server_ip_address[255];

#define INVALID_START_TYPE -1
#define START_AS_PARENT 1
#define START_AS_CHILD 2
static int starting_node_type = INVALID_START_TYPE;

static my_list list_of_keys;
static my_list trash_both;

#define NORMAL_NODE 0
#define SPLITTING_PARENT_INIT 1
#define SPLITTING_PARENT_MIGRATING 2
#define SPLITTING_CHILD_INIT 3
#define SPLITTING_CHILD_MIGRATING 4
#define MERGING_PARENT_INIT 5
#define MERGING_PARENT_MIGRATING 6
#define MERGING_CHILD_INIT 7
#define MERGING_CHILD_MIGRATING 8

static int mode;
static pthread_key_t set_command_to_execute_t;
static pthread_key_t key_to_transfer_t;

/* This reduces the latency without adding lots of extra wiring to be able to
 * notify the listener thread of when to listen again.
 * Also, the clock timer could be broken out into its own thread and we
 * can block the listener via a condition.
 */
static volatile bool allow_new_conns = true;
static struct event maxconnsevent;
static void maxconns_handler(const int fd, const short which, void *arg) {
	struct timeval t = { .tv_sec = 0, .tv_usec = 10000 };

	if (fd == -42 || allow_new_conns == false) {
		/* reschedule in 10ms if we need to keep polling */
		evtimer_set(&maxconnsevent, maxconns_handler, 0);
		event_base_set(main_base, &maxconnsevent);
		evtimer_add(&maxconnsevent, &t);
	} else {
		evtimer_del(&maxconnsevent);
		accept_new_conns(true);
	}
}

#define REALTIME_MAXDELTA 60*60*24*30

/*
 * given time value that's either unix time or delta from current unix time, return
 * unix time. Use the fact that delta can't exceed one month (and real time value can't
 * be that low).
 */
static rel_time_t realtime(const time_t exptime) {
	/* no. of seconds in 30 days - largest possible delta exptime */

	if (exptime == 0)
		return 0; /* 0 means never expire */

	if (exptime > REALTIME_MAXDELTA) {
		/* if item expiration is at/before the server started, give it an
		 expiration time of 1 second after the server started.
		 (because 0 means don't expire).  without this, we'd
		 underflow and wrap around to some large value way in the
		 future, effectively making items expiring in the past
		 really expiring never */
		if (exptime <= process_started)
			return (rel_time_t) 1;
		return (rel_time_t) (exptime - process_started);
	} else {
		return (rel_time_t) (exptime + current_time);
	}
}

static void stats_init(void) {
	stats.curr_items = stats.total_items = stats.curr_conns =
			stats.total_conns = stats.conn_structs = 0;
	stats.get_cmds = stats.set_cmds = stats.get_hits = stats.get_misses =
			stats.evictions = stats.reclaimed = 0;
	stats.touch_cmds = stats.touch_misses = stats.touch_hits =
			stats.rejected_conns = 0;
	stats.curr_bytes = stats.listen_disabled_num = 0;
	stats.hash_power_level = stats.hash_bytes = stats.hash_is_expanding = 0;
	stats.expired_unfetched = stats.evicted_unfetched = 0;
	stats.slabs_moved = 0;
	stats.accepting_conns = true; /* assuming we start in this state. */
	stats.slab_reassign_running = false;

	/* make the time we started always be 2 seconds before we really
	 did, so time(0) - time.started is never zero.  if so, things
	 like 'settings.oldest_live' which act as booleans as well as
	 values are now false in boolean context... */
	process_started = time(0) - 2;
	stats_prefix_init();
}

static void stats_reset(void) {
	STATS_LOCK();
	stats.total_items = stats.total_conns = 0;
	stats.rejected_conns = 0;
	stats.evictions = 0;
	stats.reclaimed = 0;
	stats.listen_disabled_num = 0;
	stats_prefix_clear();
	STATS_UNLOCK();
	threadlocal_stats_reset();
	item_stats_reset();
}

static void settings_init(void) {
	settings.use_cas = true;
	settings.access = 0700;
	settings.port = 11211;
	settings.udpport = 11211;
	/* By default this string should be NULL for getaddrinfo() */
	settings.inter = NULL;
	settings.maxbytes = 64 * 1024 * 1024; /* default is 64MB */
	settings.maxconns = 1024; /* to limit connections-related memory to about 5MB */
	settings.verbose = 0;
	settings.oldest_live = 0;
	settings.evict_to_free = 1; /* push old items out of cache when memory runs out */
	settings.socketpath = NULL; /* by default, not using a unix socket */
	settings.factor = 1.25;
	settings.chunk_size = 48; /* space for a modest key and value */
	settings.num_threads = 4; /* N workers */
	settings.num_threads_per_udp = 0;
	settings.prefix_delimiter = ':';
	settings.detail_enabled = 0;
	settings.reqs_per_event = 20;
	settings.backlog = 1024;
	settings.binding_protocol = negotiating_prot;
	settings.item_size_max = 1024 * 1024; /* The famous 1MB upper limit. */
	settings.maxconns_fast = false;
	settings.hashpower_init = 0;
	settings.slab_reassign = false;
	settings.slab_automove = 0;
	settings.shutdown_command = false;
}

/*
 * Adds a message header to a connection.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */
static int add_msghdr(conn *c) {
	struct msghdr *msg;

	assert(c != NULL);

	if (c->msgsize == c->msgused) {
		msg = realloc(c->msglist, c->msgsize * 2 * sizeof(struct msghdr));
		if (!msg)
			return -1;
		c->msglist = msg;
		c->msgsize *= 2;
	}

	msg = c->msglist + c->msgused;

	/* this wipes msg_iovlen, msg_control, msg_controllen, and
	 msg_flags, the last 3 of which aren't defined on solaris: */
	memset(msg, 0, sizeof(struct msghdr));

	msg->msg_iov = &c->iov[c->iovused];

	if (c->request_addr_size > 0) {
		msg->msg_name = &c->request_addr;
		msg->msg_namelen = c->request_addr_size;
	}

	c->msgbytes = 0;
	c->msgused++;

	if (IS_UDP(c->transport)) {
		/* Leave room for the UDP header, which we'll fill in later. */
		return add_iov(c, NULL, UDP_HEADER_SIZE);
	}

	return 0;
}

/*
 * Free list management for connections.
 */

static conn **freeconns;
static int freetotal;
static int freecurr;
/* Lock for connection freelist */
static pthread_mutex_t conn_lock = PTHREAD_MUTEX_INITIALIZER;

static void conn_init(void) {
	freetotal = 200;
	freecurr = 0;
	if ((freeconns = calloc(freetotal, sizeof(conn *))) == NULL ) {
		fprintf(stderr, "Failed to allocate connection structures\n");
	}
	return;
}

/*
 * Returns a connection from the freelist, if any.
 */
conn *conn_from_freelist() {
	conn *c;

	pthread_mutex_lock(&conn_lock);
	if (freecurr > 0) {
		c = freeconns[--freecurr];
	} else {
		c = NULL;
	}
	pthread_mutex_unlock(&conn_lock);

	return c;
}

/*
 * Adds a connection to the freelist. 0 = success.
 */
bool conn_add_to_freelist(conn *c) {
	bool ret = true;
	pthread_mutex_lock(&conn_lock);
	if (freecurr < freetotal) {
		freeconns[freecurr++] = c;
		ret = false;
	} else {
		/* try to enlarge free connections array */
		size_t newsize = freetotal * 2;
		conn **new_freeconns = realloc(freeconns, sizeof(conn *) * newsize);
		if (new_freeconns) {
			freetotal = newsize;
			freeconns = new_freeconns;
			freeconns[freecurr++] = c;
			ret = false;
		}
	}
	pthread_mutex_unlock(&conn_lock);
	return ret;
}

static const char *prot_text(enum protocol prot) {
	char *rv = "unknown";
	switch (prot) {
	case ascii_prot:
		rv = "ascii";
		break;
	case binary_prot:
		rv = "binary";
		break;
	case negotiating_prot:
		rv = "auto-negotiate";
		break;
	}
	return rv;
}

conn *conn_new(const int sfd, enum conn_states init_state,
		const int event_flags, const int read_buffer_size,
		enum network_transport transport, struct event_base *base) {
	conn *c = conn_from_freelist();

	if (NULL == c) {
		if (!(c = (conn *) calloc(1, sizeof(conn)))) {
			fprintf(stderr, "calloc()\n");
			return NULL ;
		}MEMCACHED_CONN_CREATE(c);

		c->rbuf = c->wbuf = 0;
		c->ilist = 0;
		c->suffixlist = 0;
		c->iov = 0;
		c->msglist = 0;
		c->hdrbuf = 0;

		c->rsize = read_buffer_size;
		c->wsize = DATA_BUFFER_SIZE;
		c->isize = ITEM_LIST_INITIAL;
		c->suffixsize = SUFFIX_LIST_INITIAL;
		c->iovsize = IOV_LIST_INITIAL;
		c->msgsize = MSG_LIST_INITIAL;
		c->hdrsize = 0;

		c->rbuf = (char *) malloc((size_t) c->rsize);
		c->wbuf = (char *) malloc((size_t) c->wsize);
		c->ilist = (item **) malloc(sizeof(item *) * c->isize);
		c->suffixlist = (char **) malloc(sizeof(char *) * c->suffixsize);
		c->iov = (struct iovec *) malloc(sizeof(struct iovec) * c->iovsize);
		c->msglist = (struct msghdr *) malloc(
				sizeof(struct msghdr) * c->msgsize);

		if (c->rbuf == 0 || c->wbuf == 0 || c->ilist == 0 || c->iov == 0
				|| c->msglist == 0 || c->suffixlist == 0) {
			conn_free(c);
			fprintf(stderr, "malloc()\n");
			return NULL ;
		}

		STATS_LOCK();
		stats.conn_structs++;
		STATS_UNLOCK();
	}

	c->transport = transport;
	c->protocol = settings.binding_protocol;

	/* unix socket mode doesn't need this, so zeroed out.  but why
	 * is this done for every command?  presumably for UDP
	 * mode.  */
	if (!settings.socketpath) {
		c->request_addr_size = sizeof(c->request_addr);
	} else {
		c->request_addr_size = 0;
	}

	if (settings.verbose > 1) {
		if (init_state == conn_listening) {
			fprintf(stderr, "<%d server listening (%s)\n", sfd,
					prot_text(c->protocol));
		} else if (IS_UDP(transport)) {
			fprintf(stderr, "<%d server listening (udp)\n", sfd);
		} else if (c->protocol == negotiating_prot) {
			fprintf(stderr, "<%d new auto-negotiating client connection\n",
					sfd);
		} else if (c->protocol == ascii_prot) {
			fprintf(stderr, "<%d new ascii client connection.\n", sfd);
		} else if (c->protocol == binary_prot) {
			fprintf(stderr, "<%d new binary client connection.\n", sfd);
		} else {
			fprintf(stderr, "<%d new unknown (%d) client connection\n", sfd,
					c->protocol);
			assert(false);
		}
	}

	c->sfd = sfd;
	c->state = init_state;
	c->rlbytes = 0;
	c->cmd = -1;
	c->rbytes = c->wbytes = 0;
	c->wcurr = c->wbuf;
	c->rcurr = c->rbuf;
	c->ritem = 0;
	c->icurr = c->ilist;
	c->suffixcurr = c->suffixlist;
	c->ileft = 0;
	c->suffixleft = 0;
	c->iovused = 0;
	c->msgcurr = 0;
	c->msgused = 0;

	c->write_and_go = init_state;
	c->write_and_free = 0;
	c->item = 0;

	c->noreply = false;

	event_set(&c->event, sfd, event_flags, event_handler, (void *) c);
	event_base_set(base, &c->event);
	c->ev_flags = event_flags;

	if (event_add(&c->event, 0) == -1) {
		if (conn_add_to_freelist(c)) {
			conn_free(c);
		}
		perror("event_add");
		return NULL ;
	}

	STATS_LOCK();
	stats.curr_conns++;
	stats.total_conns++;
	STATS_UNLOCK();

	MEMCACHED_CONN_ALLOCATE(c->sfd);

	return c;
}

static void conn_cleanup(conn *c) {
	assert(c != NULL);

	if (c->item) {
		item_remove(c->item);
		c->item = 0;
	}

	if (c->ileft != 0) {
		for (; c->ileft > 0; c->ileft--, c->icurr++) {
			item_remove(*(c->icurr));
		}
	}

	if (c->suffixleft != 0) {
		for (; c->suffixleft > 0; c->suffixleft--, c->suffixcurr++) {
			cache_free(c->thread->suffix_cache, *(c->suffixcurr));
		}
	}

	if (c->write_and_free) {
		free(c->write_and_free);
		c->write_and_free = 0;
	}

	if (c->sasl_conn) {
		assert(settings.sasl);
		sasl_dispose(&c->sasl_conn);
		c->sasl_conn = NULL;
	}

	if (IS_UDP(c->transport)) {
		conn_set_state(c, conn_read);
	}
}

/*
 * Frees a connection.
 */
void conn_free(conn *c) {
	if (c) {
		MEMCACHED_CONN_DESTROY(c);
		if (c->hdrbuf)
			free(c->hdrbuf);
		if (c->msglist)
			free(c->msglist);
		if (c->rbuf)
			free(c->rbuf);
		if (c->wbuf)
			free(c->wbuf);
		if (c->ilist)
			free(c->ilist);
		if (c->suffixlist)
			free(c->suffixlist);
		if (c->iov)
			free(c->iov);
		free(c);
	}
}

static void conn_close(conn *c) {
	assert(c != NULL);

	/* delete the event, the socket and the conn */
	event_del(&c->event);

	if (settings.verbose > 1)
		fprintf(stderr, "<%d connection closed.\n", c->sfd);

	MEMCACHED_CONN_RELEASE(c->sfd);
	close(c->sfd);
	pthread_mutex_lock(&conn_lock);
	allow_new_conns = true;
	pthread_mutex_unlock(&conn_lock);
	conn_cleanup(c);

	/* if the connection has big buffers, just free it */
	if (c->rsize > READ_BUFFER_HIGHWAT || conn_add_to_freelist(c)) {
		conn_free(c);
	}

	STATS_LOCK();
	stats.curr_conns--;
	STATS_UNLOCK();

	return;
}

/*
 * Shrinks a connection's buffers if they're too big.  This prevents
 * periodic large "get" requests from permanently chewing lots of server
 * memory.
 *
 * This should only be called in between requests since it can wipe output
 * buffers!
 */
static void conn_shrink(conn *c) {
	assert(c != NULL);

	if (IS_UDP(c->transport))
		return;

	if (c->rsize > READ_BUFFER_HIGHWAT && c->rbytes < DATA_BUFFER_SIZE) {
		char *newbuf;

		if (c->rcurr != c->rbuf)
			memmove(c->rbuf, c->rcurr, (size_t) c->rbytes);

		newbuf = (char *) realloc((void *) c->rbuf, DATA_BUFFER_SIZE);

		if (newbuf) {
			c->rbuf = newbuf;
			c->rsize = DATA_BUFFER_SIZE;
		}
		/* TODO check other branch... */
		c->rcurr = c->rbuf;
	}

	if (c->isize > ITEM_LIST_HIGHWAT) {
		item **newbuf = (item**) realloc((void *) c->ilist,
				ITEM_LIST_INITIAL * sizeof(c->ilist[0]));
		if (newbuf) {
			c->ilist = newbuf;
			c->isize = ITEM_LIST_INITIAL;
		}
		/* TODO check error condition? */
	}

	if (c->msgsize > MSG_LIST_HIGHWAT) {
		struct msghdr *newbuf = (struct msghdr *) realloc((void *) c->msglist,
				MSG_LIST_INITIAL * sizeof(c->msglist[0]));
		if (newbuf) {
			c->msglist = newbuf;
			c->msgsize = MSG_LIST_INITIAL;
		}
		/* TODO check error condition? */
	}

	if (c->iovsize > IOV_LIST_HIGHWAT) {
		struct iovec *newbuf = (struct iovec *) realloc((void *) c->iov,
				IOV_LIST_INITIAL * sizeof(c->iov[0]));
		if (newbuf) {
			c->iov = newbuf;
			c->iovsize = IOV_LIST_INITIAL;
		}
		/* TODO check return value */
	}
}

/**
 * Convert a state name to a human readable form.
 */
static const char *state_text(enum conn_states state) {
	const char* const statenames[] = { "conn_listening", "conn_new_cmd",
			"conn_waiting", "conn_read", "conn_parse_cmd", "conn_write",
			"conn_nread", "conn_swallow", "conn_closing", "conn_mwrite" };
	return statenames[state];
}

/*
 * Sets a connection's current state in the state machine. Any special
 * processing that needs to happen on certain state transitions can
 * happen here.
 */
static void conn_set_state(conn *c, enum conn_states state) {
	assert(c != NULL);
	assert(state >= conn_listening && state < conn_max_state);

	if (state != c->state) {
		if (settings.verbose > 2) {
			fprintf(stderr, "%d: going from %s to %s\n", c->sfd,
					state_text(c->state), state_text(state));
		}

		if (state == conn_write || state == conn_mwrite) {
			MEMCACHED_PROCESS_COMMAND_END(c->sfd, c->wbuf, c->wbytes);
		}
		c->state = state;
	}
}

/*
 * Ensures that there is room for another struct iovec in a connection's
 * iov list.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */
static int ensure_iov_space(conn *c) {
	assert(c != NULL);

	if (c->iovused >= c->iovsize) {
		int i, iovnum;
		struct iovec *new_iov = (struct iovec *) realloc(c->iov,
				(c->iovsize * 2) * sizeof(struct iovec));
		if (!new_iov)
			return -1;
		c->iov = new_iov;
		c->iovsize *= 2;

		/* Point all the msghdr structures at the new list. */
		for (i = 0, iovnum = 0; i < c->msgused; i++) {
			c->msglist[i].msg_iov = &c->iov[iovnum];
			iovnum += c->msglist[i].msg_iovlen;
		}
	}

	return 0;
}

/*
 * Adds data to the list of pending data that will be written out to a
 * connection.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */

static int add_iov(conn *c, const void *buf, int len) {
	struct msghdr *m;
	int leftover;
	bool limit_to_mtu;

	assert(c != NULL);

	do {
		m = &c->msglist[c->msgused - 1];

		/*
		 * Limit UDP packets, and the first payloads of TCP replies, to
		 * UDP_MAX_PAYLOAD_SIZE bytes.
		 */
		limit_to_mtu = IS_UDP(c->transport) || (1 == c->msgused);

		/* We may need to start a new msghdr if this one is full. */
		if (m->msg_iovlen == IOV_MAX
				|| (limit_to_mtu && c->msgbytes >= UDP_MAX_PAYLOAD_SIZE)) {
			add_msghdr(c);
			m = &c->msglist[c->msgused - 1];
		}

		if (ensure_iov_space(c) != 0)
			return -1;

		/* If the fragment is too big to fit in the datagram, split it up */
		if (limit_to_mtu && len + c->msgbytes > UDP_MAX_PAYLOAD_SIZE) {
			leftover = len + c->msgbytes - UDP_MAX_PAYLOAD_SIZE;
			len -= leftover;
		} else {
			leftover = 0;
		}

		m = &c->msglist[c->msgused - 1];
		m->msg_iov[m->msg_iovlen].iov_base = (void *) buf;
		m->msg_iov[m->msg_iovlen].iov_len = len;

		c->msgbytes += len;
		c->iovused++;
		m->msg_iovlen++;

		buf = ((char *) buf) + len;
		len = leftover;
	} while (leftover > 0);

	return 0;
}

/*
 * Constructs a set of UDP headers and attaches them to the outgoing messages.
 */
static int build_udp_headers(conn *c) {
	int i;
	unsigned char *hdr;

	assert(c != NULL);

	if (c->msgused > c->hdrsize) {
		void *new_hdrbuf;
		if (c->hdrbuf)
			new_hdrbuf = realloc(c->hdrbuf, c->msgused * 2 * UDP_HEADER_SIZE);
		else
			new_hdrbuf = malloc(c->msgused * 2 * UDP_HEADER_SIZE);
		if (!new_hdrbuf)
			return -1;
		c->hdrbuf = (unsigned char *) new_hdrbuf;
		c->hdrsize = c->msgused * 2;
	}

	hdr = c->hdrbuf;
	for (i = 0; i < c->msgused; i++) {
		c->msglist[i].msg_iov[0].iov_base = (void*) hdr;
		c->msglist[i].msg_iov[0].iov_len = UDP_HEADER_SIZE;
		*hdr++ = c->request_id / 256;
		*hdr++ = c->request_id % 256;
		*hdr++ = i / 256;
		*hdr++ = i % 256;
		*hdr++ = c->msgused / 256;
		*hdr++ = c->msgused % 256;
		*hdr++ = 0;
		*hdr++ = 0;
		assert(
				(void *) hdr == (caddr_t)c->msglist[i].msg_iov[0].iov_base + UDP_HEADER_SIZE);
	}

	return 0;
}

static void out_string(conn *c, const char *str) {
	size_t len;

	assert(c != NULL);

	if (c->noreply) {
		if (settings.verbose > 1)
			fprintf(stderr, ">%d NOREPLY %s\n", c->sfd, str);
		c->noreply = false;
		conn_set_state(c, conn_new_cmd);
		return;
	}

	if (settings.verbose > 1)
		fprintf(stderr, ">%d %s\n", c->sfd, str);

	/* Nuke a partial output... */
	c->msgcurr = 0;
	c->msgused = 0;
	c->iovused = 0;
	add_msghdr(c);

	len = strlen(str);
	if ((len + 2) > c->wsize) {
		/* ought to be always enough. just fail for simplicity */
		str = "SERVER_ERROR output line too long";
		len = strlen(str);
	}

	memcpy(c->wbuf, str, len);
	memcpy(c->wbuf + len, "\r\n", 2);
	c->wbytes = len + 2;
	c->wcurr = c->wbuf;

	conn_set_state(c, conn_write);
	c->write_and_go = conn_new_cmd;
	return;
}

/*
 * we get here after reading the value in set/add/replace commands. The command
 * has been stored in c->cmd, and the item is ready in c->item.
 */
static void complete_nread_ascii(conn *c) {
	assert(c != NULL);

	item *it = c->item;
	int comm = c->cmd;
	enum store_item_type ret;

	pthread_mutex_lock(&c->thread->stats.mutex);
	c->thread->stats.slab_stats[it->slabs_clsid].set_cmds++;
	pthread_mutex_unlock(&c->thread->stats.mutex);

	if (strncmp(ITEM_data(it) + it->nbytes - 2, "\r\n", 2) != 0) {
		out_string(c, "CLIENT_ERROR bad data chunk");
	} else {
		ret = store_item(it, comm, c);

#ifdef ENABLE_DTRACE
		uint64_t cas = ITEM_get_cas(it);
		switch (c->cmd) {
			case NREAD_ADD:
			MEMCACHED_COMMAND_ADD(c->sfd, ITEM_key(it), it->nkey,
					(ret == 1) ? it->nbytes : -1, cas);
			break;
			case NREAD_REPLACE:
			MEMCACHED_COMMAND_REPLACE(c->sfd, ITEM_key(it), it->nkey,
					(ret == 1) ? it->nbytes : -1, cas);
			break;
			case NREAD_APPEND:
			MEMCACHED_COMMAND_APPEND(c->sfd, ITEM_key(it), it->nkey,
					(ret == 1) ? it->nbytes : -1, cas);
			break;
			case NREAD_PREPEND:
			MEMCACHED_COMMAND_PREPEND(c->sfd, ITEM_key(it), it->nkey,
					(ret == 1) ? it->nbytes : -1, cas);
			break;
			case NREAD_SET:
			MEMCACHED_COMMAND_SET(c->sfd, ITEM_key(it), it->nkey,
					(ret == 1) ? it->nbytes : -1, cas);
			break;
			case NREAD_CAS:
			MEMCACHED_COMMAND_CAS(c->sfd, ITEM_key(it), it->nkey, it->nbytes,
					cas);
			break;
		}
#endif

		switch (ret) {
		case STORED:
			out_string(c, "STORED");
			break;
		case EXISTS:
			out_string(c, "EXISTS");
			break;
		case NOT_FOUND:
			out_string(c, "NOT_FOUND");
			break;
		case NOT_STORED:
			out_string(c, "NOT_STORED");
			break;
		default:
			out_string(c, "SERVER_ERROR Unhandled storage type.");
		}

	}

	item_remove(c->item); /* release the c->item reference */
	c->item = 0;
}

/**
 * get a pointer to the start of the request struct for the current command
 */
static void* binary_get_request(conn *c) {
	char *ret = c->rcurr;
	ret -= (sizeof(c->binary_header) + c->binary_header.request.keylen
			+ c->binary_header.request.extlen);

	assert(ret >= c->rbuf);
	return ret;
}

/**
 * get a pointer to the key in this request
 */
static char* binary_get_key(conn *c) {
	return c->rcurr - (c->binary_header.request.keylen);
}

static void add_bin_header(conn *c, uint16_t err, uint8_t hdr_len,
		uint16_t key_len, uint32_t body_len) {
	protocol_binary_response_header* header;

	assert(c);

	c->msgcurr = 0;
	c->msgused = 0;
	c->iovused = 0;
	if (add_msghdr(c) != 0) {
		/* XXX:  out_string is inappropriate here */
		out_string(c, "SERVER_ERROR out of memory");
		return;
	}

	header = (protocol_binary_response_header *) c->wbuf;

	header->response.magic = (uint8_t) PROTOCOL_BINARY_RES;
	header->response.opcode = c->binary_header.request.opcode;
	header->response.keylen = (uint16_t) htons(key_len);

	header->response.extlen = (uint8_t) hdr_len;
	header->response.datatype = (uint8_t) PROTOCOL_BINARY_RAW_BYTES;
	header->response.status = (uint16_t) htons(err);

	header->response.bodylen = htonl(body_len);
	header->response.opaque = c->opaque;
	header->response.cas = htonll(c->cas);

	if (settings.verbose > 1) {
		int ii;
		fprintf(stderr, ">%d Writing bin response:", c->sfd);
		for (ii = 0; ii < sizeof(header->bytes); ++ii) {
			if (ii % 4 == 0) {
				fprintf(stderr, "\n>%d  ", c->sfd);
			}
			fprintf(stderr, " 0x%02x", header->bytes[ii]);
		}
		fprintf(stderr, "\n");
	}

	add_iov(c, c->wbuf, sizeof(header->response));
}

static void write_bin_error(conn *c, protocol_binary_response_status err,
		int swallow) {
	const char *errstr = "Unknown error";
	size_t len;

	switch (err) {
	case PROTOCOL_BINARY_RESPONSE_ENOMEM:
		errstr = "Out of memory";
		break;
	case PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND:
		errstr = "Unknown command";
		break;
	case PROTOCOL_BINARY_RESPONSE_KEY_ENOENT:
		errstr = "Not found";
		break;
	case PROTOCOL_BINARY_RESPONSE_EINVAL:
		errstr = "Invalid arguments";
		break;
	case PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS:
		errstr = "Data exists for key.";
		break;
	case PROTOCOL_BINARY_RESPONSE_E2BIG:
		errstr = "Too large.";
		break;
	case PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL:
		errstr = "Non-numeric server-side value for incr or decr";
		break;
	case PROTOCOL_BINARY_RESPONSE_NOT_STORED:
		errstr = "Not stored.";
		break;
	case PROTOCOL_BINARY_RESPONSE_AUTH_ERROR:
		errstr = "Auth failure.";
		break;
	default:
		assert(false);
		errstr = "UNHANDLED ERROR";
		fprintf(stderr, ">%d UNHANDLED ERROR: %d\n", c->sfd, err);
	}

	if (settings.verbose > 1) {
		fprintf(stderr, ">%d Writing an error: %s\n", c->sfd, errstr);
	}

	len = strlen(errstr);
	add_bin_header(c, err, 0, 0, len);
	if (len > 0) {
		add_iov(c, errstr, len);
	}
	conn_set_state(c, conn_mwrite);
	if (swallow > 0) {
		c->sbytes = swallow;
		c->write_and_go = conn_swallow;
	} else {
		c->write_and_go = conn_new_cmd;
	}
}

/* Form and send a response to a command over the binary protocol */
static void write_bin_response(conn *c, void *d, int hlen, int keylen, int dlen) {
	if (!c->noreply || c->cmd == PROTOCOL_BINARY_CMD_GET
			|| c->cmd == PROTOCOL_BINARY_CMD_GETK) {
		add_bin_header(c, 0, hlen, keylen, dlen);
		if (dlen > 0) {
			add_iov(c, d, dlen);
		}
		conn_set_state(c, conn_mwrite);
		c->write_and_go = conn_new_cmd;
	} else {
		conn_set_state(c, conn_new_cmd);
	}
}

static void complete_incr_bin(conn *c) {
	item *it;
	char *key;
	size_t nkey;
	/* Weird magic in add_delta forces me to pad here */
	char tmpbuf[INCR_MAX_STORAGE_LEN];
	uint64_t cas = 0;

	protocol_binary_response_incr* rsp =
			(protocol_binary_response_incr*) c->wbuf;
	protocol_binary_request_incr* req = binary_get_request(c);

	assert(c != NULL);
	assert(c->wsize >= sizeof(*rsp));

	/* fix byteorder in the request */
	req->message.body.delta = ntohll(req->message.body.delta);
	req->message.body.initial = ntohll(req->message.body.initial);
	req->message.body.expiration = ntohl(req->message.body.expiration);
	key = binary_get_key(c);
	nkey = c->binary_header.request.keylen;

	if (settings.verbose > 1) {
		int i;
		fprintf(stderr, "incr ");

		for (i = 0; i < nkey; i++) {
			fprintf(stderr, "%c", key[i]);
		}
		fprintf(stderr, " %lld, %llu, %d\n",
				(long long) req->message.body.delta,
				(long long) req->message.body.initial,
				req->message.body.expiration);
	}

	if (c->binary_header.request.cas != 0) {
		cas = c->binary_header.request.cas;
	}
	switch (add_delta(c, key, nkey, c->cmd == PROTOCOL_BINARY_CMD_INCREMENT,
			req->message.body.delta, tmpbuf, &cas)) {
	case OK:
		rsp->message.body.value = htonll(strtoull(tmpbuf, NULL, 10));
		if (cas) {
			c->cas = cas;
		}
		write_bin_response(c, &rsp->message.body, 0, 0,
				sizeof(rsp->message.body.value));
		break;
	case NON_NUMERIC:
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL, 0);
		break;
	case EOM:
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
		break;
	case DELTA_ITEM_NOT_FOUND:
		if (req->message.body.expiration != 0xffffffff) {
			/* Save some room for the response */
			rsp->message.body.value = htonll(req->message.body.initial);
			it = item_alloc(key, nkey, 0,
					realtime(req->message.body.expiration),
					INCR_MAX_STORAGE_LEN);

			if (it != NULL ) {
				snprintf(ITEM_data(it), INCR_MAX_STORAGE_LEN, "%llu",
						(unsigned long long) req->message.body.initial);

				if (store_item(it, NREAD_ADD, c)) {
					c->cas = ITEM_get_cas(it);
					write_bin_response(c, &rsp->message.body, 0, 0,
							sizeof(rsp->message.body.value));
				} else {
					write_bin_error(c, PROTOCOL_BINARY_RESPONSE_NOT_STORED, 0);
				}
				item_remove(it); /* release our reference */
			} else {
				write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
			}
		} else {
			pthread_mutex_lock(&c->thread->stats.mutex);
			if (c->cmd == PROTOCOL_BINARY_CMD_INCREMENT) {
				c->thread->stats.incr_misses++;
			} else {
				c->thread->stats.decr_misses++;
			}
			pthread_mutex_unlock(&c->thread->stats.mutex);

			write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
		}
		break;
	case DELTA_ITEM_CAS_MISMATCH:
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, 0);
		break;
	}
}

static void complete_update_bin(conn *c) {
	protocol_binary_response_status eno = PROTOCOL_BINARY_RESPONSE_EINVAL;
	enum store_item_type ret = NOT_STORED;
	assert(c != NULL);

	item *it = c->item;

	pthread_mutex_lock(&c->thread->stats.mutex);
	c->thread->stats.slab_stats[it->slabs_clsid].set_cmds++;
	pthread_mutex_unlock(&c->thread->stats.mutex);

	/* We don't actually receive the trailing two characters in the bin
	 * protocol, so we're going to just set them here */
	*(ITEM_data(it) + it->nbytes - 2) = '\r';
	*(ITEM_data(it) + it->nbytes - 1) = '\n';

	ret = store_item(it, c->cmd, c);

#ifdef ENABLE_DTRACE
	uint64_t cas = ITEM_get_cas(it);
	switch (c->cmd) {
		case NREAD_ADD:
		MEMCACHED_COMMAND_ADD(c->sfd, ITEM_key(it), it->nkey,
				(ret == STORED) ? it->nbytes : -1, cas);
		break;
		case NREAD_REPLACE:
		MEMCACHED_COMMAND_REPLACE(c->sfd, ITEM_key(it), it->nkey,
				(ret == STORED) ? it->nbytes : -1, cas);
		break;
		case NREAD_APPEND:
		MEMCACHED_COMMAND_APPEND(c->sfd, ITEM_key(it), it->nkey,
				(ret == STORED) ? it->nbytes : -1, cas);
		break;
		case NREAD_PREPEND:
		MEMCACHED_COMMAND_PREPEND(c->sfd, ITEM_key(it), it->nkey,
				(ret == STORED) ? it->nbytes : -1, cas);
		break;
		case NREAD_SET:
		MEMCACHED_COMMAND_SET(c->sfd, ITEM_key(it), it->nkey,
				(ret == STORED) ? it->nbytes : -1, cas);
		break;
	}
#endif

	switch (ret) {
	case STORED:
		/* Stored */
		write_bin_response(c, NULL, 0, 0, 0);
		break;
	case EXISTS:
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, 0);
		break;
	case NOT_FOUND:
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
		break;
	case NOT_STORED:
		if (c->cmd == NREAD_ADD) {
			eno = PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
		} else if (c->cmd == NREAD_REPLACE) {
			eno = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
		} else {
			eno = PROTOCOL_BINARY_RESPONSE_NOT_STORED;
		}
		write_bin_error(c, eno, 0);
	}

	item_remove(c->item); /* release the c->item reference */
	c->item = 0;
}

static void process_bin_touch(conn *c) {
	item *it;

	protocol_binary_response_get* rsp = (protocol_binary_response_get*) c->wbuf;
	char* key = binary_get_key(c);
	size_t nkey = c->binary_header.request.keylen;
	protocol_binary_request_touch *t = binary_get_request(c);
	time_t exptime = ntohl(t->message.body.expiration);

	if (settings.verbose > 1) {
		int ii;
		/* May be GAT/GATQ/etc */
		fprintf(stderr, "<%d TOUCH ", c->sfd);
		for (ii = 0; ii < nkey; ++ii) {
			fprintf(stderr, "%c", key[ii]);
		}
		fprintf(stderr, "\n");
	}

	it = item_touch(key, nkey, realtime(exptime));

	if (it) {
		/* the length has two unnecessary bytes ("\r\n") */
		uint16_t keylen = 0;
		uint32_t bodylen = sizeof(rsp->message.body) + (it->nbytes - 2);

		item_update(it);
		pthread_mutex_lock(&c->thread->stats.mutex);
		c->thread->stats.touch_cmds++;
		c->thread->stats.slab_stats[it->slabs_clsid].touch_hits++;
		pthread_mutex_unlock(&c->thread->stats.mutex);

		MEMCACHED_COMMAND_TOUCH(c->sfd, ITEM_key(it), it->nkey,
				it->nbytes, ITEM_get_cas(it));

		if (c->cmd == PROTOCOL_BINARY_CMD_TOUCH) {
			bodylen -= it->nbytes - 2;
		} else if (c->cmd == PROTOCOL_BINARY_CMD_GATK) {
			bodylen += nkey;
			keylen = nkey;
		}

		add_bin_header(c, 0, sizeof(rsp->message.body), keylen, bodylen);
		rsp->message.header.response.cas = htonll(ITEM_get_cas(it));

		// add the flags
		rsp->message.body.flags = htonl(strtoul(ITEM_suffix(it), NULL, 10));
		add_iov(c, &rsp->message.body, sizeof(rsp->message.body));

		if (c->cmd == PROTOCOL_BINARY_CMD_GATK) {
			add_iov(c, ITEM_key(it), nkey);
		}

		/* Add the data minus the CRLF */
		if (c->cmd != PROTOCOL_BINARY_CMD_TOUCH) {
			add_iov(c, ITEM_data(it), it->nbytes - 2);
		}

		conn_set_state(c, conn_mwrite);
		c->write_and_go = conn_new_cmd;
		/* Remember this command so we can garbage collect it later */
		c->item = it;
	} else {
		pthread_mutex_lock(&c->thread->stats.mutex);
		c->thread->stats.touch_cmds++;
		c->thread->stats.touch_misses++;
		pthread_mutex_unlock(&c->thread->stats.mutex);

		MEMCACHED_COMMAND_TOUCH(c->sfd, key, nkey, -1, 0);

		if (c->noreply) {
			conn_set_state(c, conn_new_cmd);
		} else {
			if (c->cmd == PROTOCOL_BINARY_CMD_GATK) {
				char *ofs = c->wbuf + sizeof(protocol_binary_response_header);
				add_bin_header(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0, nkey,
						nkey);
				memcpy(ofs, key, nkey);
				add_iov(c, ofs, nkey);
				conn_set_state(c, conn_mwrite);
				c->write_and_go = conn_new_cmd;
			} else {
				write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
			}
		}
	}

	if (settings.detail_enabled) {
		stats_prefix_record_get(key, nkey, NULL != it);
	}
}

static void process_bin_get(conn *c) {
	item *it;

	protocol_binary_response_get* rsp = (protocol_binary_response_get*) c->wbuf;
	char* key = binary_get_key(c);
	size_t nkey = c->binary_header.request.keylen;

	if (settings.verbose > 1) {
		int ii;
		fprintf(stderr, "<%d GET ", c->sfd);
		for (ii = 0; ii < nkey; ++ii) {
			fprintf(stderr, "%c", key[ii]);
		}
		fprintf(stderr, "\n");
	}

	it = item_get(key, nkey);
	if (it) {
		/* the length has two unnecessary bytes ("\r\n") */
		uint16_t keylen = 0;
		uint32_t bodylen = sizeof(rsp->message.body) + (it->nbytes - 2);

		item_update(it);
		pthread_mutex_lock(&c->thread->stats.mutex);
		c->thread->stats.get_cmds++;
		c->thread->stats.slab_stats[it->slabs_clsid].get_hits++;
		pthread_mutex_unlock(&c->thread->stats.mutex);

		MEMCACHED_COMMAND_GET(c->sfd, ITEM_key(it), it->nkey,
				it->nbytes, ITEM_get_cas(it));

		if (c->cmd == PROTOCOL_BINARY_CMD_GETK) {
			bodylen += nkey;
			keylen = nkey;
		}
		add_bin_header(c, 0, sizeof(rsp->message.body), keylen, bodylen);
		rsp->message.header.response.cas = htonll(ITEM_get_cas(it));

		// add the flags
		rsp->message.body.flags = htonl(strtoul(ITEM_suffix(it), NULL, 10));
		add_iov(c, &rsp->message.body, sizeof(rsp->message.body));

		if (c->cmd == PROTOCOL_BINARY_CMD_GETK) {
			add_iov(c, ITEM_key(it), nkey);
		}

		/* Add the data minus the CRLF */
		add_iov(c, ITEM_data(it), it->nbytes - 2);
		conn_set_state(c, conn_mwrite);
		c->write_and_go = conn_new_cmd;
		/* Remember this command so we can garbage collect it later */
		c->item = it;
	} else {
		pthread_mutex_lock(&c->thread->stats.mutex);
		c->thread->stats.get_cmds++;
		c->thread->stats.get_misses++;
		pthread_mutex_unlock(&c->thread->stats.mutex);

		MEMCACHED_COMMAND_GET(c->sfd, key, nkey, -1, 0);

		if (c->noreply) {
			conn_set_state(c, conn_new_cmd);
		} else {
			if (c->cmd == PROTOCOL_BINARY_CMD_GETK) {
				char *ofs = c->wbuf + sizeof(protocol_binary_response_header);
				add_bin_header(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0, nkey,
						nkey);
				memcpy(ofs, key, nkey);
				add_iov(c, ofs, nkey);
				conn_set_state(c, conn_mwrite);
				c->write_and_go = conn_new_cmd;
			} else {
				write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
			}
		}
	}

	if (settings.detail_enabled) {
		stats_prefix_record_get(key, nkey, NULL != it);
	}
}

static void append_bin_stats(const char *key, const uint16_t klen,
		const char *val, const uint32_t vlen, conn *c) {
	char *buf = c->stats.buffer + c->stats.offset;
	uint32_t bodylen = klen + vlen;
	protocol_binary_response_header header = { .response.magic =
			(uint8_t) PROTOCOL_BINARY_RES, .response.opcode =
			PROTOCOL_BINARY_CMD_STAT, .response.keylen = (uint16_t) htons(klen),
			.response.datatype = (uint8_t) PROTOCOL_BINARY_RAW_BYTES,
			.response.bodylen = htonl(bodylen), .response.opaque = c->opaque };

	memcpy(buf, header.bytes, sizeof(header.response));
	buf += sizeof(header.response);

	if (klen > 0) {
		memcpy(buf, key, klen);
		buf += klen;

		if (vlen > 0) {
			memcpy(buf, val, vlen);
		}
	}

	c->stats.offset += sizeof(header.response) + bodylen;
}

static void append_ascii_stats(const char *key, const uint16_t klen,
		const char *val, const uint32_t vlen, conn *c) {
	char *pos = c->stats.buffer + c->stats.offset;
	uint32_t nbytes = 0;
	int remaining = c->stats.size - c->stats.offset;
	int room = remaining - 1;

	if (klen == 0 && vlen == 0) {
		nbytes = snprintf(pos, room, "END\r\n");
	} else if (vlen == 0) {
		nbytes = snprintf(pos, room, "STAT %s\r\n", key);
	} else {
		nbytes = snprintf(pos, room, "STAT %s %s\r\n", key, val);
	}

	c->stats.offset += nbytes;
}

static bool grow_stats_buf(conn *c, size_t needed) {
	size_t nsize = c->stats.size;
	size_t available = nsize - c->stats.offset;
	bool rv = true;

	/* Special case: No buffer -- need to allocate fresh */
	if (c->stats.buffer == NULL ) {
		nsize = 1024;
		available = c->stats.size = c->stats.offset = 0;
	}

	while (needed > available) {
		assert(nsize > 0);
		nsize = nsize << 1;
		available = nsize - c->stats.offset;
	}

	if (nsize != c->stats.size) {
		char *ptr = realloc(c->stats.buffer, nsize);
		if (ptr) {
			c->stats.buffer = ptr;
			c->stats.size = nsize;
		} else {
			rv = false;
		}
	}

	return rv;
}

static void append_stats(const char *key, const uint16_t klen, const char *val,
		const uint32_t vlen, const void *cookie) {
	/* value without a key is invalid */
	if (klen == 0 && vlen > 0) {
		return;
	}

	conn *c = (conn*) cookie;

	if (c->protocol == binary_prot) {
		size_t needed = vlen + klen + sizeof(protocol_binary_response_header);
		if (!grow_stats_buf(c, needed)) {
			return;
		}
		append_bin_stats(key, klen, val, vlen, c);
	} else {
		size_t needed = vlen + klen + 10; // 10 == "STAT = \r\n"
		if (!grow_stats_buf(c, needed)) {
			return;
		}
		append_ascii_stats(key, klen, val, vlen, c);
	}

	assert(c->stats.offset <= c->stats.size);
}

static void process_bin_stat(conn *c) {
	char *subcommand = binary_get_key(c);
	size_t nkey = c->binary_header.request.keylen;

	if (settings.verbose > 1) {
		int ii;
		fprintf(stderr, "<%d STATS ", c->sfd);
		for (ii = 0; ii < nkey; ++ii) {
			fprintf(stderr, "%c", subcommand[ii]);
		}
		fprintf(stderr, "\n");
	}

	if (nkey == 0) {
		/* request all statistics */
		server_stats(&append_stats, c);
		(void) get_stats(NULL, 0, &append_stats, c);
	} else if (strncmp(subcommand, "reset", 5) == 0) {
		stats_reset();
	} else if (strncmp(subcommand, "settings", 8) == 0) {
		process_stat_settings(&append_stats, c);
	} else if (strncmp(subcommand, "detail", 6) == 0) {
		char *subcmd_pos = subcommand + 6;
		if (strncmp(subcmd_pos, " dump", 5) == 0) {
			int len;
			char *dump_buf = stats_prefix_dump(&len);
			if (dump_buf == NULL || len <= 0) {
				write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
				return;
			} else {
				append_stats("detailed", strlen("detailed"), dump_buf, len, c);
				free(dump_buf);
			}
		} else if (strncmp(subcmd_pos, " on", 3) == 0) {
			settings.detail_enabled = 1;
		} else if (strncmp(subcmd_pos, " off", 4) == 0) {
			settings.detail_enabled = 0;
		} else {
			write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
			return;
		}
	} else {
		if (get_stats(subcommand, nkey, &append_stats, c)) {
			if (c->stats.buffer == NULL ) {
				write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
			} else {
				write_and_free(c, c->stats.buffer, c->stats.offset);
				c->stats.buffer = NULL;
			}
		} else {
			write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
		}

		return;
	}

	/* Append termination package and start the transfer */
	append_stats(NULL, 0, NULL, 0, c);
	if (c->stats.buffer == NULL ) {
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
	} else {
		write_and_free(c, c->stats.buffer, c->stats.offset);
		c->stats.buffer = NULL;
	}
}

static void bin_read_key(conn *c, enum bin_substates next_substate, int extra) {
	assert(c);
	c->substate = next_substate;
	c->rlbytes = c->keylen + extra;

	/* Ok... do we have room for the extras and the key in the input buffer? */
	ptrdiff_t offset = c->rcurr + sizeof(protocol_binary_request_header)
			- c->rbuf;
	if (c->rlbytes > c->rsize - offset) {
		size_t nsize = c->rsize;
		size_t size = c->rlbytes + sizeof(protocol_binary_request_header);

		while (size > nsize) {
			nsize *= 2;
		}

		if (nsize != c->rsize) {
			if (settings.verbose > 1) {
				fprintf(stderr, "%d: Need to grow buffer from %lu to %lu\n",
						c->sfd, (unsigned long) c->rsize,
						(unsigned long) nsize);
			}
			char *newm = realloc(c->rbuf, nsize);
			if (newm == NULL ) {
				if (settings.verbose) {
					fprintf(stderr,
							"%d: Failed to grow buffer.. closing connection\n",
							c->sfd);
				}
				conn_set_state(c, conn_closing);
				return;
			}

			c->rbuf = newm;
			/* rcurr should point to the same offset in the packet */
			c->rcurr = c->rbuf + offset
					- sizeof(protocol_binary_request_header);
			c->rsize = nsize;
		}
		if (c->rbuf != c->rcurr) {
			memmove(c->rbuf, c->rcurr, c->rbytes);
			c->rcurr = c->rbuf;
			if (settings.verbose > 1) {
				fprintf(stderr, "%d: Repack input buffer\n", c->sfd);
			}
		}
	}

	/* preserve the header in the buffer.. */
	c->ritem = c->rcurr + sizeof(protocol_binary_request_header);
	conn_set_state(c, conn_nread);
}

/* Just write an error message and disconnect the client */
static void handle_binary_protocol_error(conn *c) {
	write_bin_error(c, PROTOCOL_BINARY_RESPONSE_EINVAL, 0);
	if (settings.verbose) {
		fprintf(stderr, "Protocol error (opcode %02x), close connection %d\n",
				c->binary_header.request.opcode, c->sfd);
	}
	c->write_and_go = conn_closing;
}

static void init_sasl_conn(conn *c) {
	assert(c);
	/* should something else be returned? */
	if (!settings.sasl)
		return;

	if (!c->sasl_conn) {
		int result = sasl_server_new("memcached",
				NULL,
				my_sasl_hostname[0] ? my_sasl_hostname : NULL,
				NULL, NULL,
				NULL, 0, &c->sasl_conn);
		if (result != SASL_OK) {
			if (settings.verbose) {
				fprintf(stderr, "Failed to initialize SASL conn.\n");
			}
			c->sasl_conn = NULL;
		}
	}
}

static void bin_list_sasl_mechs(conn *c) {
	// Guard against a disabled SASL.
	if (!settings.sasl) {
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND,
				c->binary_header.request.bodylen
						- c->binary_header.request.keylen);
		return;
	}

	init_sasl_conn(c);
	const char *result_string = NULL;
	unsigned int string_length = 0;
	int result = sasl_listmech(c->sasl_conn, NULL,
			"", /* What to prepend the string with */
			" ", /* What to separate mechanisms with */
			"", /* What to append to the string */
			&result_string, &string_length,
			NULL);
	if (result != SASL_OK) {
		/* Perhaps there's a better error for this... */
		if (settings.verbose) {
			fprintf(stderr, "Failed to list SASL mechanisms.\n");
		}
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, 0);
		return;
	}
	write_bin_response(c, (char*) result_string, 0, 0, string_length);
}

static void process_bin_sasl_auth(conn *c) {
	// Guard for handling disabled SASL on the server.
	if (!settings.sasl) {
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND,
				c->binary_header.request.bodylen
						- c->binary_header.request.keylen);
		return;
	}

	assert(c->binary_header.request.extlen == 0);

	int nkey = c->binary_header.request.keylen;
	int vlen = c->binary_header.request.bodylen - nkey;

	if (nkey > MAX_SASL_MECH_LEN) {
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_EINVAL, vlen);
		c->write_and_go = conn_swallow;
		return;
	}

	char *key = binary_get_key(c);
	assert(key);

	item *it = item_alloc(key, nkey, 0, 0, vlen);

	if (it == 0) {
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, vlen);
		c->write_and_go = conn_swallow;
		return;
	}

	c->item = it;
	c->ritem = ITEM_data(it);
	c->rlbytes = vlen;
	conn_set_state(c, conn_nread);
	c->substate = bin_reading_sasl_auth_data;
}

static void process_bin_complete_sasl_auth(conn *c) {
	assert(settings.sasl);
	const char *out = NULL;
	unsigned int outlen = 0;

	assert(c->item);
	init_sasl_conn(c);

	int nkey = c->binary_header.request.keylen;
	int vlen = c->binary_header.request.bodylen - nkey;

	char mech[nkey + 1];
	memcpy(mech, ITEM_key((item*)c->item), nkey);
	mech[nkey] = 0x00;

	if (settings.verbose)
		fprintf(stderr, "mech:  ``%s'' with %d bytes of data\n", mech, vlen);

	const char *challenge = vlen == 0 ? NULL : ITEM_data((item*) c->item);

	int result = -1;

	switch (c->cmd) {
	case PROTOCOL_BINARY_CMD_SASL_AUTH:
		result = sasl_server_start(c->sasl_conn, mech,
				challenge, vlen,
				&out, &outlen);
		break;
	case PROTOCOL_BINARY_CMD_SASL_STEP:
		result = sasl_server_step(c->sasl_conn,
				challenge, vlen,
				&out, &outlen);
		break;
	default:
		assert(false);
		/* CMD should be one of the above */
		/* This code is pretty much impossible, but makes the compiler
		 happier */
		if (settings.verbose) {
			fprintf(stderr, "Unhandled command %d with challenge %s\n", c->cmd,
					challenge);
		}
		break;
	}

	item_unlink(c->item);

	if (settings.verbose) {
		fprintf(stderr, "sasl result code:  %d\n", result);
	}

	switch (result) {
	case SASL_OK:
		write_bin_response(c, "Authenticated", 0, 0, strlen("Authenticated"));
		pthread_mutex_lock(&c->thread->stats.mutex);
		c->thread->stats.auth_cmds++;
		pthread_mutex_unlock(&c->thread->stats.mutex);
		break;
	case SASL_CONTINUE:
		add_bin_header(c, PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE, 0, 0, outlen);
		if (outlen > 0) {
			add_iov(c, out, outlen);
		}
		conn_set_state(c, conn_mwrite);
		c->write_and_go = conn_new_cmd;
		break;
	default:
		if (settings.verbose)
			fprintf(stderr, "Unknown sasl response:  %d\n", result);
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, 0);
		pthread_mutex_lock(&c->thread->stats.mutex);
		c->thread->stats.auth_cmds++;
		c->thread->stats.auth_errors++;
		pthread_mutex_unlock(&c->thread->stats.mutex);
	}
}

static bool authenticated(conn *c) {
	assert(settings.sasl);
	bool rv = false;

	switch (c->cmd) {
	case PROTOCOL_BINARY_CMD_SASL_LIST_MECHS: /* FALLTHROUGH */
	case PROTOCOL_BINARY_CMD_SASL_AUTH: /* FALLTHROUGH */
	case PROTOCOL_BINARY_CMD_SASL_STEP: /* FALLTHROUGH */
	case PROTOCOL_BINARY_CMD_VERSION: /* FALLTHROUGH */
		rv = true;
		break;
	default:
		if (c->sasl_conn) {
			const void *uname = NULL;
			sasl_getprop(c->sasl_conn, SASL_USERNAME, &uname);
			rv = uname != NULL;
		}
	}

	if (settings.verbose > 1) {
		fprintf(stderr, "authenticated() in cmd 0x%02x is %s\n", c->cmd,
				rv ? "true" : "false");
	}

	return rv;
}

static void dispatch_bin_command(conn *c) {
	int protocol_error = 0;

	int extlen = c->binary_header.request.extlen;
	int keylen = c->binary_header.request.keylen;
	uint32_t bodylen = c->binary_header.request.bodylen;

	if (settings.sasl && !authenticated(c)) {
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, 0);
		c->write_and_go = conn_closing;
		return;
	}

	MEMCACHED_PROCESS_COMMAND_START(c->sfd, c->rcurr, c->rbytes);
	c->noreply = true;

	/* binprot supports 16bit keys, but internals are still 8bit */
	if (keylen > KEY_MAX_LENGTH) {
		handle_binary_protocol_error(c);
		return;
	}

	switch (c->cmd) {
	case PROTOCOL_BINARY_CMD_SETQ:
		c->cmd = PROTOCOL_BINARY_CMD_SET;
		break;
	case PROTOCOL_BINARY_CMD_ADDQ:
		c->cmd = PROTOCOL_BINARY_CMD_ADD;
		break;
	case PROTOCOL_BINARY_CMD_REPLACEQ:
		c->cmd = PROTOCOL_BINARY_CMD_REPLACE;
		break;
	case PROTOCOL_BINARY_CMD_DELETEQ:
		c->cmd = PROTOCOL_BINARY_CMD_DELETE;
		break;
	case PROTOCOL_BINARY_CMD_INCREMENTQ:
		c->cmd = PROTOCOL_BINARY_CMD_INCREMENT;
		break;
	case PROTOCOL_BINARY_CMD_DECREMENTQ:
		c->cmd = PROTOCOL_BINARY_CMD_DECREMENT;
		break;
	case PROTOCOL_BINARY_CMD_QUITQ:
		c->cmd = PROTOCOL_BINARY_CMD_QUIT;
		break;
	case PROTOCOL_BINARY_CMD_FLUSHQ:
		c->cmd = PROTOCOL_BINARY_CMD_FLUSH;
		break;
	case PROTOCOL_BINARY_CMD_APPENDQ:
		c->cmd = PROTOCOL_BINARY_CMD_APPEND;
		break;
	case PROTOCOL_BINARY_CMD_PREPENDQ:
		c->cmd = PROTOCOL_BINARY_CMD_PREPEND;
		break;
	case PROTOCOL_BINARY_CMD_GETQ:
		c->cmd = PROTOCOL_BINARY_CMD_GET;
		break;
	case PROTOCOL_BINARY_CMD_GETKQ:
		c->cmd = PROTOCOL_BINARY_CMD_GETK;
		break;
	case PROTOCOL_BINARY_CMD_GATQ:
		c->cmd = PROTOCOL_BINARY_CMD_GAT;
		break;
	case PROTOCOL_BINARY_CMD_GATKQ:
		c->cmd = PROTOCOL_BINARY_CMD_GAT;
		break;
	default:
		c->noreply = false;
	}

	switch (c->cmd) {
	case PROTOCOL_BINARY_CMD_VERSION:
		if (extlen == 0 && keylen == 0 && bodylen == 0) {
			write_bin_response(c, VERSION, 0, 0, strlen(VERSION));
		} else {
			protocol_error = 1;
		}
		break;
	case PROTOCOL_BINARY_CMD_FLUSH:
		if (keylen == 0 && bodylen == extlen && (extlen == 0 || extlen == 4)) {
			bin_read_key(c, bin_read_flush_exptime, extlen);
		} else {
			protocol_error = 1;
		}
		break;
	case PROTOCOL_BINARY_CMD_NOOP:
		if (extlen == 0 && keylen == 0 && bodylen == 0) {
			write_bin_response(c, NULL, 0, 0, 0);
		} else {
			protocol_error = 1;
		}
		break;
	case PROTOCOL_BINARY_CMD_SET: /* FALLTHROUGH */
	case PROTOCOL_BINARY_CMD_ADD: /* FALLTHROUGH */
	case PROTOCOL_BINARY_CMD_REPLACE:
		if (extlen == 8 && keylen != 0 && bodylen >= (keylen + 8)) {
			bin_read_key(c, bin_reading_set_header, 8);
		} else {
			protocol_error = 1;
		}
		break;
	case PROTOCOL_BINARY_CMD_GETQ: /* FALLTHROUGH */
	case PROTOCOL_BINARY_CMD_GET: /* FALLTHROUGH */
	case PROTOCOL_BINARY_CMD_GETKQ: /* FALLTHROUGH */
	case PROTOCOL_BINARY_CMD_GETK:
		if (extlen == 0 && bodylen == keylen && keylen > 0) {
			bin_read_key(c, bin_reading_get_key, 0);
		} else {
			protocol_error = 1;
		}
		break;
	case PROTOCOL_BINARY_CMD_DELETE:
		if (keylen > 0 && extlen == 0 && bodylen == keylen) {
			bin_read_key(c, bin_reading_del_header, extlen);
		} else {
			protocol_error = 1;
		}
		break;
	case PROTOCOL_BINARY_CMD_INCREMENT:
	case PROTOCOL_BINARY_CMD_DECREMENT:
		if (keylen > 0 && extlen == 20 && bodylen == (keylen + extlen)) {
			bin_read_key(c, bin_reading_incr_header, 20);
		} else {
			protocol_error = 1;
		}
		break;
	case PROTOCOL_BINARY_CMD_APPEND:
	case PROTOCOL_BINARY_CMD_PREPEND:
		if (keylen > 0 && extlen == 0) {
			bin_read_key(c, bin_reading_set_header, 0);
		} else {
			protocol_error = 1;
		}
		break;
	case PROTOCOL_BINARY_CMD_STAT:
		if (extlen == 0) {
			bin_read_key(c, bin_reading_stat, 0);
		} else {
			protocol_error = 1;
		}
		break;
	case PROTOCOL_BINARY_CMD_QUIT:
		if (keylen == 0 && extlen == 0 && bodylen == 0) {
			write_bin_response(c, NULL, 0, 0, 0);
			c->write_and_go = conn_closing;
			if (c->noreply) {
				conn_set_state(c, conn_closing);
			}
		} else {
			protocol_error = 1;
		}
		break;
	case PROTOCOL_BINARY_CMD_SASL_LIST_MECHS:
		if (extlen == 0 && keylen == 0 && bodylen == 0) {
			bin_list_sasl_mechs(c);
		} else {
			protocol_error = 1;
		}
		break;
	case PROTOCOL_BINARY_CMD_SASL_AUTH:
	case PROTOCOL_BINARY_CMD_SASL_STEP:
		if (extlen == 0 && keylen != 0) {
			bin_read_key(c, bin_reading_sasl_auth, 0);
		} else {
			protocol_error = 1;
		}
		break;
	case PROTOCOL_BINARY_CMD_TOUCH:
	case PROTOCOL_BINARY_CMD_GAT:
	case PROTOCOL_BINARY_CMD_GATQ:
	case PROTOCOL_BINARY_CMD_GATK:
	case PROTOCOL_BINARY_CMD_GATKQ:
		if (extlen == 4 && keylen != 0) {
			bin_read_key(c, bin_reading_touch_key, 4);
		} else {
			protocol_error = 1;
		}
		break;
	default:
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND, bodylen);
	}

	if (protocol_error)
		handle_binary_protocol_error(c);
}

static void process_bin_update(conn *c) {
	char *key;
	int nkey;
	int vlen;
	item *it;
	protocol_binary_request_set* req = binary_get_request(c);

	assert(c != NULL);

	key = binary_get_key(c);
	nkey = c->binary_header.request.keylen;

	/* fix byteorder in the request */
	req->message.body.flags = ntohl(req->message.body.flags);
	req->message.body.expiration = ntohl(req->message.body.expiration);

	vlen = c->binary_header.request.bodylen
			- (nkey + c->binary_header.request.extlen);

	if (settings.verbose > 1) {
		int ii;
		if (c->cmd == PROTOCOL_BINARY_CMD_ADD) {
			fprintf(stderr, "<%d ADD ", c->sfd);
		} else if (c->cmd == PROTOCOL_BINARY_CMD_SET) {
			fprintf(stderr, "<%d SET ", c->sfd);
		} else {
			fprintf(stderr, "<%d REPLACE ", c->sfd);
		}
		for (ii = 0; ii < nkey; ++ii) {
			fprintf(stderr, "%c", key[ii]);
		}

		fprintf(stderr, " Value len is %d", vlen);
		fprintf(stderr, "\n");
	}

	if (settings.detail_enabled) {
		stats_prefix_record_set(key, nkey);
	}

	it = item_alloc(key, nkey, req->message.body.flags,
			realtime(req->message.body.expiration), vlen + 2);

	if (it == 0) {
		if (!item_size_ok(nkey, req->message.body.flags, vlen + 2)) {
			write_bin_error(c, PROTOCOL_BINARY_RESPONSE_E2BIG, vlen);
		} else {
			write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, vlen);
		}

		/* Avoid stale data persisting in cache because we failed alloc.
		 * Unacceptable for SET. Anywhere else too? */
		if (c->cmd == PROTOCOL_BINARY_CMD_SET) {
			it = item_get(key, nkey);
			if (it) {
				item_unlink(it);
				item_remove(it);
			}
		}

		/* swallow the data line */
		c->write_and_go = conn_swallow;
		return;
	}

	ITEM_set_cas(it, c->binary_header.request.cas);

	switch (c->cmd) {
	case PROTOCOL_BINARY_CMD_ADD:
		c->cmd = NREAD_ADD;
		break;
	case PROTOCOL_BINARY_CMD_SET:
		c->cmd = NREAD_SET;
		break;
	case PROTOCOL_BINARY_CMD_REPLACE:
		c->cmd = NREAD_REPLACE;
		break;
	default:
		assert(0);
	}

	if (ITEM_get_cas(it) != 0) {
		c->cmd = NREAD_CAS;
	}

	c->item = it;
	c->ritem = ITEM_data(it);
	c->rlbytes = vlen;
	conn_set_state(c, conn_nread);
	c->substate = bin_read_set_value;
}

static void process_bin_append_prepend(conn *c) {
	char *key;
	int nkey;
	int vlen;
	item *it;

	assert(c != NULL);

	key = binary_get_key(c);
	nkey = c->binary_header.request.keylen;
	vlen = c->binary_header.request.bodylen - nkey;

	if (settings.verbose > 1) {
		fprintf(stderr, "Value len is %d\n", vlen);
	}

	if (settings.detail_enabled) {
		stats_prefix_record_set(key, nkey);
	}

	it = item_alloc(key, nkey, 0, 0, vlen + 2);

	if (it == 0) {
		if (!item_size_ok(nkey, 0, vlen + 2)) {
			write_bin_error(c, PROTOCOL_BINARY_RESPONSE_E2BIG, vlen);
		} else {
			write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, vlen);
		}
		/* swallow the data line */
		c->write_and_go = conn_swallow;
		return;
	}

	ITEM_set_cas(it, c->binary_header.request.cas);

	switch (c->cmd) {
	case PROTOCOL_BINARY_CMD_APPEND:
		c->cmd = NREAD_APPEND;
		break;
	case PROTOCOL_BINARY_CMD_PREPEND:
		c->cmd = NREAD_PREPEND;
		break;
	default:
		assert(0);
	}

	c->item = it;
	c->ritem = ITEM_data(it);
	c->rlbytes = vlen;
	conn_set_state(c, conn_nread);
	c->substate = bin_read_set_value;
}

static void process_bin_flush(conn *c) {
	time_t exptime = 0;
	protocol_binary_request_flush* req = binary_get_request(c);

	if (c->binary_header.request.extlen == sizeof(req->message.body)) {
		exptime = ntohl(req->message.body.expiration);
	}

	if (exptime > 0) {
		settings.oldest_live = realtime(exptime) - 1;
	} else {
		settings.oldest_live = current_time - 1;
	}
	item_flush_expired();

	pthread_mutex_lock(&c->thread->stats.mutex);
	c->thread->stats.flush_cmds++;
	pthread_mutex_unlock(&c->thread->stats.mutex);

	write_bin_response(c, NULL, 0, 0, 0);
}

static void process_bin_delete(conn *c) {
	item *it;

	protocol_binary_request_delete* req = binary_get_request(c);

	char* key = binary_get_key(c);
	size_t nkey = c->binary_header.request.keylen;

	assert(c != NULL);

	if (settings.verbose > 1) {
		fprintf(stderr, "Deleting %s\n", key);
	}

	if (settings.detail_enabled) {
		stats_prefix_record_delete(key, nkey);
	}

	it = item_get(key, nkey);
	if (it) {
		uint64_t cas = ntohll(req->message.header.request.cas);
		if (cas == 0 || cas == ITEM_get_cas(it)) {
			MEMCACHED_COMMAND_DELETE(c->sfd, ITEM_key(it), it->nkey);
			pthread_mutex_lock(&c->thread->stats.mutex);
			c->thread->stats.slab_stats[it->slabs_clsid].delete_hits++;
			pthread_mutex_unlock(&c->thread->stats.mutex);
			item_unlink(it);
			write_bin_response(c, NULL, 0, 0, 0);
		} else {
			write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, 0);
		}
		item_remove(it); /* release our reference */
	} else {
		write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
		pthread_mutex_lock(&c->thread->stats.mutex);
		c->thread->stats.delete_misses++;
		pthread_mutex_unlock(&c->thread->stats.mutex);
	}
}

static void complete_nread_binary(conn *c) {
	assert(c != NULL);
	assert(c->cmd >= 0);

	switch (c->substate) {
	case bin_reading_set_header:
		if (c->cmd == PROTOCOL_BINARY_CMD_APPEND
				|| c->cmd == PROTOCOL_BINARY_CMD_PREPEND) {
			process_bin_append_prepend(c);
		} else {
			process_bin_update(c);
		}
		break;
	case bin_read_set_value:
		complete_update_bin(c);
		break;
	case bin_reading_get_key:
		process_bin_get(c);
		break;
	case bin_reading_touch_key:
		process_bin_touch(c);
		break;
	case bin_reading_stat:
		process_bin_stat(c);
		break;
	case bin_reading_del_header:
		process_bin_delete(c);
		break;
	case bin_reading_incr_header:
		complete_incr_bin(c);
		break;
	case bin_read_flush_exptime:
		process_bin_flush(c);
		break;
	case bin_reading_sasl_auth:
		process_bin_sasl_auth(c);
		break;
	case bin_reading_sasl_auth_data:
		process_bin_complete_sasl_auth(c);
		break;
	default:
		fprintf(stderr, "Not handling substate %d\n", c->substate);
		assert(0);
	}
}

static void reset_cmd_handler(conn *c) {
	c->cmd = -1;
	c->substate = bin_no_state;
	if (c->item != NULL ) {
		item_remove(c->item);
		c->item = NULL;
	}
	conn_shrink(c);
	if (c->rbytes > 0) {
		conn_set_state(c, conn_parse_cmd);
	} else {
		conn_set_state(c, conn_waiting);
	}
}

static void complete_nread(conn *c) {
	assert(c != NULL);
	assert(c->protocol == ascii_prot || c->protocol == binary_prot);

	if (c->protocol == ascii_prot) {
		complete_nread_ascii(c);
	} else if (c->protocol == binary_prot) {
		complete_nread_binary(c);
	}
}

/*
 * Stores an item in the cache according to the semantics of one of the set
 * commands. In threaded mode, this is protected by the cache lock.
 *
 * Returns the state of storage.
 */
enum store_item_type do_store_item(item *it, int comm, conn *c,
		const uint32_t hv) {
	char *key = ITEM_key(it);
	item *old_it = do_item_get(key, it->nkey, hv);
	enum store_item_type stored = NOT_STORED;

	item *new_it = NULL;
	int flags;

	if (old_it != NULL && comm == NREAD_ADD) {
		/* add only adds a nonexistent item, but promote to head of LRU */
		do_item_update(old_it);
	} else if (!old_it
			&& (comm == NREAD_REPLACE || comm == NREAD_APPEND
					|| comm == NREAD_PREPEND)) {
		/* replace only replaces an existing value; don't store */
	} else if (comm == NREAD_CAS) {
		/* validate cas operation */
		if (old_it == NULL ) {
			// LRU expired
			stored = NOT_FOUND;
			pthread_mutex_lock(&c->thread->stats.mutex);
			c->thread->stats.cas_misses++;
			pthread_mutex_unlock(&c->thread->stats.mutex);
		} else if (ITEM_get_cas(it) == ITEM_get_cas(old_it)) {
			// cas validates
			// it and old_it may belong to different classes.
			// I'm updating the stats for the one that's getting pushed out
			pthread_mutex_lock(&c->thread->stats.mutex);
			c->thread->stats.slab_stats[old_it->slabs_clsid].cas_hits++;
			pthread_mutex_unlock(&c->thread->stats.mutex);

			item_replace(old_it, it, hv);
			stored = STORED;
		} else {
			pthread_mutex_lock(&c->thread->stats.mutex);
			c->thread->stats.slab_stats[old_it->slabs_clsid].cas_badval++;
			pthread_mutex_unlock(&c->thread->stats.mutex);

			if (settings.verbose > 1) {
				fprintf(stderr, "CAS:  failure: expected %llu, got %llu\n",
						(unsigned long long) ITEM_get_cas(old_it),
						(unsigned long long) ITEM_get_cas(it));
			}
			stored = EXISTS;
		}
	} else {
		/*
		 * Append - combine new and old record into single one. Here it's
		 * atomic and thread-safe.
		 */
		if (comm == NREAD_APPEND || comm == NREAD_PREPEND) {
			/*
			 * Validate CAS
			 */
			if (ITEM_get_cas(it) != 0) {
				// CAS much be equal
				if (ITEM_get_cas(it) != ITEM_get_cas(old_it)) {
					stored = EXISTS;
				}
			}

			if (stored == NOT_STORED) {
				/* we have it and old_it here - alloc memory to hold both */
				/* flags was already lost - so recover them from ITEM_suffix(it) */

				flags = (int) strtol(ITEM_suffix(old_it), (char **) NULL, 10);

				new_it = do_item_alloc(key, it->nkey, flags, old_it->exptime,
						it->nbytes + old_it->nbytes - 2 /* CRLF */, hv);

				if (new_it == NULL ) {
					/* SERVER_ERROR out of memory */
					if (old_it != NULL )
						do_item_remove(old_it);

					return NOT_STORED;
				}

				/* copy data from it and old_it to new_it */

				if (comm == NREAD_APPEND) {
					memcpy(ITEM_data(new_it), ITEM_data(old_it),
							old_it->nbytes);
					memcpy(ITEM_data(new_it) + old_it->nbytes - 2 /* CRLF */,
							ITEM_data(it), it->nbytes);
				} else {
					/* NREAD_PREPEND */
					memcpy(ITEM_data(new_it), ITEM_data(it), it->nbytes);
					memcpy(ITEM_data(new_it) + it->nbytes - 2 /* CRLF */,
							ITEM_data(old_it), old_it->nbytes);
				}

				it = new_it;
			}
		}

		if (stored == NOT_STORED) {
			if (old_it != NULL )
				item_replace(old_it, it, hv);
			else
				do_item_link(it, hv);

			c->cas = ITEM_get_cas(it);

			stored = STORED;
		}
	}

	if (old_it != NULL )
		do_item_remove(old_it); /* release our reference */
	if (new_it != NULL )
		do_item_remove(new_it);

	if (stored == STORED) {
		c->cas = ITEM_get_cas(it);
	}

	return stored;
}

typedef struct token_s {
	char *value;
	size_t length;
} token_t;

#define COMMAND_TOKEN 0
#define SUBCOMMAND_TOKEN 1
#define KEY_TOKEN 1

#define MAX_TOKENS 8

/*
 * Tokenize the command string by replacing whitespace with '\0' and update
 * the token array tokens with pointer to start of each token and length.
 * Returns total number of tokens.  The last valid token is the terminal
 * token (value points to the first unprocessed character of the string and
 * length zero).
 *
 * Usage example:
 *
 *  while(tokenize_command(command, ncommand, tokens, max_tokens) > 0) {
 *      for(int ix = 0; tokens[ix].length != 0; ix++) {
 *          ...
 *      }
 *      ncommand = tokens[ix].value - command;
 *      command  = tokens[ix].value;
 *   }
 */
static size_t tokenize_command(char *command, token_t *tokens,
		const size_t max_tokens) {
	char *s, *e;
	size_t ntokens = 0;
	size_t len = strlen(command);
	unsigned int i = 0;

	assert(command != NULL && tokens != NULL && max_tokens > 1);

	s = e = command;
	for (i = 0; i < len; i++) {
		if (*e == ' ') {
			if (s != e) {
				tokens[ntokens].value = s;
				tokens[ntokens].length = e - s;
				ntokens++;
				*e = '\0';
				if (ntokens == max_tokens - 1) {
					e++;
					s = e; /* so we don't add an extra token */
					break;
				}
			}
			s = e + 1;
		}
		e++;
	}

	if (s != e) {
		tokens[ntokens].value = s;
		tokens[ntokens].length = e - s;
		ntokens++;
	}

	/*
	 * If we scanned the whole string, the terminal value pointer is null,
	 * otherwise it is the first unprocessed character.
	 */
	tokens[ntokens].value = *e == '\0' ? NULL : e;
	tokens[ntokens].length = 0;
	ntokens++;

	return ntokens;
}

/* set up a connection to write a buffer then free it, used for stats */
static void write_and_free(conn *c, char *buf, int bytes) {
	if (buf) {
		c->write_and_free = buf;
		c->wcurr = buf;
		c->wbytes = bytes;
		conn_set_state(c, conn_write);
		c->write_and_go = conn_new_cmd;
	} else {
		out_string(c, "SERVER_ERROR out of memory writing stats");
	}
}

static inline bool set_noreply_maybe(conn *c, token_t *tokens, size_t ntokens) {
	int noreply_index = ntokens - 2;

	/*
	 NOTE: this function is not the first place where we are going to
	 send the reply.  We could send it instead from process_command()
	 if the request line has wrong number of tokens.  However parsing
	 malformed line for "noreply" option is not reliable anyway, so
	 it can't be helped.
	 */
	if (tokens[noreply_index].value
			&& strcmp(tokens[noreply_index].value, "noreply") == 0) {
		c->noreply = true;
	}
	return c->noreply;
}

void append_stat(const char *name, ADD_STAT add_stats, conn *c, const char *fmt,
		...) {
	char val_str[STAT_VAL_LEN];
	int vlen;
	va_list ap;

	assert(name);
	assert(add_stats);
	assert(c);
	assert(fmt);

	va_start(ap, fmt);
	vlen = vsnprintf(val_str, sizeof(val_str) - 1, fmt, ap);
	va_end(ap);

	add_stats(name, strlen(name), val_str, vlen, c);
}

inline static void process_stats_detail(conn *c, const char *command) {
	assert(c != NULL);

	if (strcmp(command, "on") == 0) {
		settings.detail_enabled = 1;
		out_string(c, "OK");
	} else if (strcmp(command, "off") == 0) {
		settings.detail_enabled = 0;
		out_string(c, "OK");
	} else if (strcmp(command, "dump") == 0) {
		int len;
		char *stats = stats_prefix_dump(&len);
		write_and_free(c, stats, len);
	} else {
		out_string(c, "CLIENT_ERROR usage: stats detail on|off|dump");
	}
}

/* return server specific stats only */
static void server_stats(ADD_STAT add_stats, conn *c) {
	pid_t pid = getpid();
	rel_time_t now = current_time;

	struct thread_stats thread_stats;
	threadlocal_stats_aggregate(&thread_stats);
	struct slab_stats slab_stats;
	slab_stats_aggregate(&thread_stats, &slab_stats);

#ifndef WIN32
	struct rusage usage;
	getrusage(RUSAGE_SELF, &usage);
#endif /* !WIN32 */

	STATS_LOCK();

	APPEND_STAT("pid", "%lu", (long)pid);
	APPEND_STAT("uptime", "%u", now);
	APPEND_STAT("time", "%ld", now + (long)process_started);
	APPEND_STAT("version", "%s", VERSION);
	APPEND_STAT("libevent", "%s", event_get_version());
	APPEND_STAT("pointer_size", "%d", (int)(8 * sizeof(void *)));

#ifndef WIN32
	append_stat("rusage_user", add_stats, c, "%ld.%06ld",
			(long) usage.ru_utime.tv_sec, (long) usage.ru_utime.tv_usec);
	append_stat("rusage_system", add_stats, c, "%ld.%06ld",
			(long) usage.ru_stime.tv_sec, (long) usage.ru_stime.tv_usec);
#endif /* !WIN32 */

	APPEND_STAT("curr_connections", "%u", stats.curr_conns - 1);
	APPEND_STAT("total_connections", "%u", stats.total_conns);
	if (settings.maxconns_fast) {
		APPEND_STAT("rejected_connections", "%llu",
				(unsigned long long)stats.rejected_conns);
	}
	APPEND_STAT("connection_structures", "%u", stats.conn_structs);
	APPEND_STAT("reserved_fds", "%u", stats.reserved_fds);
	APPEND_STAT("cmd_get", "%llu", (unsigned long long)thread_stats.get_cmds);
	APPEND_STAT("cmd_set", "%llu", (unsigned long long)slab_stats.set_cmds);
	APPEND_STAT("cmd_flush", "%llu",
			(unsigned long long)thread_stats.flush_cmds);
	APPEND_STAT("cmd_touch", "%llu",
			(unsigned long long)thread_stats.touch_cmds);
	APPEND_STAT("get_hits", "%llu", (unsigned long long)slab_stats.get_hits);
	APPEND_STAT("get_misses", "%llu",
			(unsigned long long)thread_stats.get_misses);
	APPEND_STAT("delete_misses", "%llu",
			(unsigned long long)thread_stats.delete_misses);
	APPEND_STAT("delete_hits", "%llu",
			(unsigned long long)slab_stats.delete_hits);
	APPEND_STAT("incr_misses", "%llu",
			(unsigned long long)thread_stats.incr_misses);
	APPEND_STAT("incr_hits", "%llu", (unsigned long long)slab_stats.incr_hits);
	APPEND_STAT("decr_misses", "%llu",
			(unsigned long long)thread_stats.decr_misses);
	APPEND_STAT("decr_hits", "%llu", (unsigned long long)slab_stats.decr_hits);
	APPEND_STAT("cas_misses", "%llu",
			(unsigned long long)thread_stats.cas_misses);
	APPEND_STAT("cas_hits", "%llu", (unsigned long long)slab_stats.cas_hits);
	APPEND_STAT("cas_badval", "%llu",
			(unsigned long long)slab_stats.cas_badval);
	APPEND_STAT("touch_hits", "%llu",
			(unsigned long long)slab_stats.touch_hits);
	APPEND_STAT("touch_misses", "%llu",
			(unsigned long long)thread_stats.touch_misses);
	APPEND_STAT("auth_cmds", "%llu",
			(unsigned long long)thread_stats.auth_cmds);
	APPEND_STAT("auth_errors", "%llu",
			(unsigned long long)thread_stats.auth_errors);
	APPEND_STAT("bytes_read", "%llu",
			(unsigned long long)thread_stats.bytes_read);
	APPEND_STAT("bytes_written", "%llu",
			(unsigned long long)thread_stats.bytes_written);
	APPEND_STAT("limit_maxbytes", "%llu",
			(unsigned long long)settings.maxbytes);
	APPEND_STAT("accepting_conns", "%u", stats.accepting_conns);
	APPEND_STAT("listen_disabled_num", "%llu",
			(unsigned long long)stats.listen_disabled_num);
	APPEND_STAT("threads", "%d", settings.num_threads);
	APPEND_STAT("conn_yields", "%llu",
			(unsigned long long)thread_stats.conn_yields);
	APPEND_STAT("hash_power_level", "%u", stats.hash_power_level);
	APPEND_STAT("hash_bytes", "%llu", (unsigned long long)stats.hash_bytes);
	APPEND_STAT("hash_is_expanding", "%u", stats.hash_is_expanding);
	if (settings.slab_reassign) {
		APPEND_STAT("slab_reassign_running", "%u", stats.slab_reassign_running);
		APPEND_STAT("slabs_moved", "%llu", stats.slabs_moved);
	}
	STATS_UNLOCK();
}

static void process_stat_settings(ADD_STAT add_stats, void *c) {
	assert(add_stats);
	APPEND_STAT("maxbytes", "%u", (unsigned int)settings.maxbytes);
	APPEND_STAT("maxconns", "%d", settings.maxconns);
	APPEND_STAT("tcpport", "%d", settings.port);
	APPEND_STAT("udpport", "%d", settings.udpport);
	APPEND_STAT("inter", "%s", settings.inter ? settings.inter : "NULL");
	APPEND_STAT("verbosity", "%d", settings.verbose);
	APPEND_STAT("oldest", "%lu", (unsigned long)settings.oldest_live);
	APPEND_STAT("evictions", "%s", settings.evict_to_free ? "on" : "off");
	APPEND_STAT("domain_socket", "%s",
			settings.socketpath ? settings.socketpath : "NULL");
	APPEND_STAT("umask", "%o", settings.access);
	APPEND_STAT("growth_factor", "%.2f", settings.factor);
	APPEND_STAT("chunk_size", "%d", settings.chunk_size);
	APPEND_STAT("num_threads", "%d", settings.num_threads);
	APPEND_STAT("num_threads_per_udp", "%d", settings.num_threads_per_udp);
	APPEND_STAT("stat_key_prefix", "%c", settings.prefix_delimiter);
	APPEND_STAT("detail_enabled", "%s", settings.detail_enabled ? "yes" : "no");
	APPEND_STAT("reqs_per_event", "%d", settings.reqs_per_event);
	APPEND_STAT("cas_enabled", "%s", settings.use_cas ? "yes" : "no");
	APPEND_STAT("tcp_backlog", "%d", settings.backlog);
	APPEND_STAT("binding_protocol", "%s", prot_text(settings.binding_protocol));
	APPEND_STAT("auth_enabled_sasl", "%s", settings.sasl ? "yes" : "no");
	APPEND_STAT("item_size_max", "%d", settings.item_size_max);
	APPEND_STAT("maxconns_fast", "%s", settings.maxconns_fast ? "yes" : "no");
	APPEND_STAT("hashpower_init", "%d", settings.hashpower_init);
	APPEND_STAT("slab_reassign", "%s", settings.slab_reassign ? "yes" : "no");
	APPEND_STAT("slab_automove", "%d", settings.slab_automove);
}

static void process_stat(conn *c, token_t *tokens, const size_t ntokens) {
	const char *subcommand = tokens[SUBCOMMAND_TOKEN].value;
	assert(c != NULL);

	if (ntokens < 2) {
		out_string(c, "CLIENT_ERROR bad command line");
		return;
	}

	if (ntokens == 2) {
		server_stats(&append_stats, c);
		(void) get_stats(NULL, 0, &append_stats, c);
	} else if (strcmp(subcommand, "reset") == 0) {
		stats_reset();
		out_string(c, "RESET");
		return;
	} else if (strcmp(subcommand, "detail") == 0) {
		/* NOTE: how to tackle detail with binary? */
		if (ntokens < 4)
			process_stats_detail(c, ""); /* outputs the error message */
		else
			process_stats_detail(c, tokens[2].value);
		/* Output already generated */
		return;
	} else if (strcmp(subcommand, "settings") == 0) {
		process_stat_settings(&append_stats, c);
	} else if (strcmp(subcommand, "cachedump") == 0) {
		char *buf;
		unsigned int bytes, id, limit = 0;

		if (ntokens < 5) {
			out_string(c, "CLIENT_ERROR bad command line");
			return;
		}

		if (!safe_strtoul(tokens[2].value, &id)
				|| !safe_strtoul(tokens[3].value, &limit)) {
			out_string(c, "CLIENT_ERROR bad command line format");
			return;
		}

		if (id >= POWER_LARGEST) {
			out_string(c, "CLIENT_ERROR Illegal slab id");
			return;
		}

		buf = item_cachedump(id, limit, &bytes);
		write_and_free(c, buf, bytes);
		return;
	} else {
		/* getting here means that the subcommand is either engine specific or
		 is invalid. query the engine and see. */
		if (get_stats(subcommand, strlen(subcommand), &append_stats, c)) {
			if (c->stats.buffer == NULL ) {
				out_string(c, "SERVER_ERROR out of memory writing stats");
			} else {
				write_and_free(c, c->stats.buffer, c->stats.offset);
				c->stats.buffer = NULL;
			}
		} else {
			out_string(c, "ERROR");
		}
		return;
	}

	/* append terminator and start the transfer */
	append_stats(NULL, 0, NULL, 0, c);

	if (c->stats.buffer == NULL ) {
		out_string(c, "SERVER_ERROR out of memory writing stats");
	} else {
		write_and_free(c, c->stats.buffer, c->stats.offset);
		c->stats.buffer = NULL;
	}
}

static unsigned long str_hash(char *str) {
	unsigned long hash = 5381;
	int c;

	while ((c = *str++) != '\0')
		hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

	return hash;
}
static Point key_point(char *key) {
	Point p;
	unsigned long hash = str_hash(key);
	p.x = hash % (int) (world_boundary.to.x);
	p.y = hash % (int) (world_boundary.to.y);
	fprintf(stderr,"Key %s projects to (%f,%f)\n",key,p.x,p.y);
	return p;
}

static int is_within_boundary(Point p, ZoneBoundary boundary) {

	if (p.x < boundary.to.x && p.y < boundary.to.y) {
		if (p.x >= boundary.from.x && p.y >= boundary.from.y)
			return 1;
	}
	return 0;

}

/*Begin list functions*/
static void mylist_print(my_list *list) {
	int i = 0;
	fprintf(stderr, "%s:(%d,[", list->name, list->size);

	for (i = 0; i < list->size; i++) {
		fprintf(stderr, "%s,", list->array[i]);
	}
	fprintf(stderr, "])\n");
}

static void mylist_init(char *name,my_list *list) {
	list->size = 0;
	strcpy(list->name,name);
}

static void mylist_add(my_list *list, char* v) {
	int new_size = list->size + 1;
	char **new_array = (char**) malloc(sizeof(char*) * new_size);
	int i = 0;
	for (i = 0; i < new_size; i++) {
		if (i == new_size - 1) {
			new_array[i] = (char*) malloc(sizeof(char*) * strlen(v));
			sprintf(new_array[i], "%s", v);
		} else {
			new_array[i] = (char*) malloc(
					sizeof(char*) * strlen(list->array[i]));
			sprintf(new_array[i], "%s", list->array[i]);
		}
	}
	list->size++;
	list->array = new_array;
	mylist_print(list);
}

static void mylist_delete_all(my_list *list) {
	list->size = 0;
	list->array = NULL;
	mylist_print(list);
}

static int mylist_contains(my_list *list, char *v) {
	int i = 0;
	for (i = 0; i < list->size; i++) {
		if (strcmp(list->array[i], v) == 0) {
			return 1;
		}
	}
	return 0;
}

static void mylist_delete(my_list *list, char* v) {
	if (mylist_contains(list, v) == 1) {
		int new_size = list->size - 1;
		char **new_array = (char**) malloc(sizeof(char*) * new_size);
		int i = 0;
		int detected = 0;
		for (i = 0; i < list->size; i++) {
			if (strcmp(list->array[i], v) == 0) {
				detected = 1;
				continue;
			}
			int index = i - detected;
			new_array[index] = (char*) malloc(
					sizeof(char*) * strlen(list->array[i]));
			sprintf(new_array[index], "%s", list->array[i]);
		}
		list->size--;
		list->array = new_array;
	}
	mylist_print(list);

}

/*Ending list functions*/

static void serialize_boundary(ZoneBoundary b, char *s) {
	sprintf(s, "[(%f,%f) to (%f,%f)]", b.from.x, b.from.y, b.to.x, b.to.y);
}

static void deserialize_boundary(char *s, ZoneBoundary *b) {
	sscanf(s, "[(%f,%f) to (%f,%f)]", &(b->from.x), &(b->from.y), &(b->to.x),
			&(b->to.y));
}
/// Start of functions common to bootstrap
static void sigchld_handler(int s) {
	while (waitpid(-1, NULL, WNOHANG) > 0);
}

// get sockaddr, IPv4 or IPv6:
static void *get_in_addr(struct sockaddr *sa) {
	if (sa->sa_family == AF_INET) {
		return &(((struct sockaddr_in*) sa)->sin_addr);
	}
	return &(((struct sockaddr_in6*) sa)->sin6_addr);
}

static int receive_connection_from_client(int server_sock_fd, char *caller){
    int new_fd; // listen on sock_fd, new connection on new_fd
    struct sockaddr_storage their_addr; // connector's address information
    socklen_t sin_size;
    char s[INET6_ADDRSTRLEN];

    fprintf(stderr,"%s : server: waiting for connections...\n",caller);
    sin_size = sizeof their_addr;
    new_fd = accept(server_sock_fd, (struct sockaddr *) &their_addr, &sin_size);
    if (new_fd == -1) {
        perror("accept");
        exit(-1);
    }

    inet_ntop(their_addr.ss_family, get_in_addr((struct sockaddr *) &their_addr), s, sizeof s);
    fprintf(stderr, "%s : server: got connection from %s\n",caller, s);
    return new_fd;
}

static int listen_on(char *port,char *caller){
    int sockfd=-1; // listen on sock_fd, new connection on new_fd
	struct sigaction sa;
   	struct addrinfo hints, *servinfo, *p;
   	int rv;

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = INADDR_ANY;

	if ((rv = getaddrinfo("localhost", port, &hints, &servinfo)) != 0) {
        fprintf(stderr, "In %s, getaddrinfo: %s\n", caller, gai_strerror(rv));
        exit(-1);
    }

    // loop through all the results and bind to the first we can
    for(p = servinfo; p != NULL; p = p->ai_next) {
        if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
            fprintf(stderr,"In %s,",caller);
            perror("listener: socket");
            continue;
        }
        if (bind(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            close(sockfd);
            fprintf(stderr,"In %s,",caller);
            perror("listener: bind");
            continue;
        }
        break;
    }
    if (p == NULL) {
        fprintf(stderr, "In %s, listener: failed to bind socket\n",caller);
        exit(-1);
    }

    if (listen(sockfd, BACKLOG) == -1) {
    fprintf(stderr,"In %s,",caller);
    perror("listen");
    exit(1);
    }

    sa.sa_handler = sigchld_handler; // reap all dead processes
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;

    if (sigaction(SIGCHLD, &sa, NULL ) == -1) {
    fprintf(stderr,"In %s,",caller);
    perror("sigaction");
    exit(1);
    }

    return sockfd;
}

static ZoneBoundary* _recv_boundary_from_neighbour(int child_fd) {
	char buf[1024];
	int MAXDATASIZE = 1024;
	memset(buf, '\0', 1024);
	if (recv(child_fd, buf, MAXDATASIZE-1, 0) == -1) {
        perror("recv");
        exit(1);
    }
	ZoneBoundary *child_boundary = (ZoneBoundary *) malloc(sizeof(ZoneBoundary));
	deserialize_boundary(buf, child_boundary);
	fprintf(stderr,"Received %s\n",buf);
	return child_boundary;
}


static float calculate_area(ZoneBoundary bounds)
{
	Point from,to;
	from.x=bounds.from.x;
	from.y=bounds.from.y;
	to.x=bounds.to.x;
	to.y=bounds.to.y;
	float area;
	fprintf(stderr,"\npoints::%f,%f,%f,%f\n",to.x,from.x,to.y,from.y);
	area= (to.x - from.x)* (to.y-from.y);
	fprintf(stderr,"%f",(to.x - from.x)*(to.y-from.y));
	return area;
}
//////////// End of functions common to bootstrap.c

static int connect_to(char *ip_address,char *port,char *caller){
    int sockfd;
    struct addrinfo hints, *servinfo, *p;
    int rv;
    char s[INET6_ADDRSTRLEN];

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    fprintf(stderr,"In %s: attempting to connect_to %s:%s\n",caller,ip_address,port);

    if ((rv = getaddrinfo(ip_address, port, &hints, &servinfo)) != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
        return -1;
    }
    for (p = servinfo; p != NULL ; p = p->ai_next) {
        if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
            fprintf(stderr,"In %s,",caller);
            perror("client: socket");
            continue;
        }
        if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            close(sockfd);
            fprintf(stderr,"In %s,",caller);
            perror("client: connect");
            continue;
        }
        break;
    }

    if (p == NULL ) {
        fprintf(stderr, "In %s : client: failed to connect\n",caller);
        exit(-1);
    }

    inet_ntop(p->ai_family, get_in_addr((struct sockaddr *) p->ai_addr), s, sizeof s);
    fprintf(stderr,"In %s : client: connected successfully to %s:%s(Ignore previous errors for this)\n", caller,s,port);
    freeaddrinfo(servinfo);
    return sockfd;
}

static int find_port(int *sock_desc){

	struct addrinfo hints, *servinfo, *p;
		int rv,addrlen;
		int socket_descriptor=0,portno;
		memset(&hints, 0, sizeof hints);
		hints.ai_family = AF_INET;
		hints.ai_socktype = SOCK_STREAM;
		hints.ai_flags = INADDR_ANY;
		struct sockaddr_in serv_addr;
	int input_portno = 0;
	char portnoString[10];
		sprintf(portnoString,"%i", input_portno);

		if ((rv = getaddrinfo(NULL, portnoString, &hints, &servinfo)) != 0) {
				fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
				//return 1;
		}

		// loop through all the results and bind to the first we can
		for(p = servinfo; p != NULL; p = p->ai_next) {
			if ((socket_descriptor = socket(p->ai_family, p->ai_socktype,
					p->ai_protocol)) == -1) {
					perror("listener: socket");
					continue;
			}
			if (bind(socket_descriptor, p->ai_addr, p->ai_addrlen) == -1) {
				close(socket_descriptor);
				perror("listener: bind");
				continue;
			}
			break;
		}
		if (p == NULL) {
			fprintf(stderr, "listener: failed to bind socket\n");
			//return 2;
		}
		addrlen = sizeof(serv_addr);
		int getsock_check=getsockname(socket_descriptor,(struct sockaddr *)&serv_addr, (socklen_t *)&addrlen) ;

		   	if (getsock_check== -1) {
		   			perror("getsockname");
		   			exit(1);
		   	}
		portno =  ntohs(serv_addr.sin_port);
        fprintf(stderr, "The actual port number is %d\n", portno);
		freeaddrinfo(servinfo);
		*sock_desc=socket_descriptor;
		return portno;
}

static void pretty_print(char *str,int len,char *caller){
    int i=0;
    char *ptr= str;
    fprintf(stderr,"%s:Character by character:\n",caller);
    for (i = 0;i<len;i++)
    {
        fprintf(stderr,"%d:%c:%d\n",i,*ptr,*ptr);
        ptr++;
    }
    ptr=str;
    fprintf(stderr,"\n%s:Single line:\n",caller);
    for (i = 0;i<len;i++)
    {
        fprintf(stderr,"%c",*ptr);
        ptr++;
    }
    fprintf(stderr,"\n%s:end of pretty print,len was %d\n",caller,len);
}

static pthread_key_t global_data_entry_t;
static char *request_neighbour(char *key, char *buf, char *type,node_info *neighbour,item* it) {
	int sockfd;
	int MAXDATASIZE = 1024;
	char str[1024];
	memset(str,'\0',1024);
	sprintf(str,"request_neighbour(type=%s,to_transfer=%s)",type,key);
    sockfd = connect_to("localhost",neighbour->request_propogation,str);

    ///imp sleep..increased two zero.
    usleep(1000);
	memset(buf, '\0', 1024);
	fprintf(stderr,"request_neighbour : sending type %s\n", type);
	send(sockfd, type, strlen(type), 0);
	///imp sleep..increased two zero.
	usleep(1000);

	memset(buf, '\0', 1024);
	fprintf(stderr,"request_neighbour : sending key/command %s\n", key);
	send(sockfd, key, strlen(key), 0);
	fprintf(stderr,"Sent command to neighbour %s\n",key);

	if(strcmp(type,"set")==0){
		///imp sleep..increased two zero.

	    usleep(1000);
        if(it){
            char *v = ITEM_data(it);
            send(sockfd,v,it->nbytes,0);
            pretty_print(v,it->nbytes,"request_neighbour,set");
            fprintf(stderr,"Sent binary value to neighbour successfully\n");
        }
        else{
            fprintf(stderr,"You should not have reached here!!!!!");
            exit(-1);
        }
	}

	//increasing 0
    usleep(1000);
    memset(buf, '\0', 1024);

	if (strcmp(type,"get")==0){
        recv(sockfd, buf, 1024,0);
        fprintf(stderr,"Received %s\n",buf);
        if(strncmp(buf,"NOT FOUND",9)){
            char *global_data_entry=(char*)malloc(sizeof(char)*1024);
            memset(global_data_entry,'\0',1024);
            recv(sockfd, global_data_entry, 1024,0);
            pthread_setspecific(global_data_entry_t,global_data_entry);
            fprintf(stderr,"get request propagation received value in binary from neighbour, value is %s\n",global_data_entry);
        }
    }
    else{
    	recv(sockfd, buf, MAXDATASIZE - 1, 0);
    }
	close(sockfd);
	return buf;
}

static void serialize_key_value_str(char *key, char *flag1, int flag2,
    int flag3, char *key_and_metadata_str) {
	sprintf(key_and_metadata_str, "%s %s %d %d", key, flag1, flag2, flag3);
	fprintf(stderr, "STRING:%s\n", key_and_metadata_str);
}

static void deserialize_key_value_str(char *key, int *flag1, int *flag2, int *flag3, char *key_and_metadata_str) {
	sscanf(key_and_metadata_str, "%s %d %d %d", key, flag1, flag2, flag3);
}

static float distance_squared(Point p1,Point p2){
    float x_component = p1.x  - p2.x;
    float y_component = p1.y  - p2.y;
    return x_component*x_component + y_component*y_component;
}

static void centroid(ZoneBoundary b,Point *c){
    c->x = b.from.x+ (b.to.x-b.from.x)/2;
    c->y = b.from.y+ (b.to.y-b.from.y)/2;
}

static node_info get_neighbour_information(char *key)
{
	int counter;
	ZoneBoundary bounds;
	Point resolved_point = key_point(key);
	node_info best_neighbour;
	float closest_distance = 99999999;
	for(counter=0;counter<10;counter++)
	{
	    if(strcmp(neighbour[counter].node_removal,"NULL") || strcmp(neighbour[counter].request_propogation,"NULL"))
        {
            bounds=neighbour[counter].boundary;
            if(is_within_boundary(resolved_point,bounds)==1)
            {
                return neighbour[counter];
            }
            else {
                Point c;
                centroid(neighbour[counter].boundary,&c);
                float distance_from_this_neighbour = distance_squared(c,resolved_point);
                fprintf(stderr,"Distance squared = %f\n",distance_from_this_neighbour);
                if(closest_distance>distance_from_this_neighbour){
                    best_neighbour = neighbour[counter];
                    closest_distance = distance_from_this_neighbour;
                }
            }
		}
	}
	fprintf(stderr,"Did not find point belonging directly onto any neighbour, propogating the request through the cluster by choosing the best neighbour\n");
	fprintf(stderr,"Chosen neighbour.request_propagation=%s\n",best_neighbour.request_propogation);
	return best_neighbour;
}

static void print_boundaries(ZoneBoundary b) {
	if (settings.verbose > 1) {
		fprintf(stderr, "[(%f,%f) to (%f,%f)]\n", b.from.x, b.from.y, b.to.x,
				b.to.y);
	}
}

static void print_all_boundaries() {
	if (settings.verbose > 1) {
		fprintf(stderr, "Current boundaries:\n");
		fprintf(stderr, "World boundary:");
		print_boundaries(world_boundary);
		fprintf(stderr, "My boundary:");
		print_boundaries(me.boundary);
		fprintf(stderr, "My new boundary:");
		print_boundaries(my_new_boundary);
	}
}

static void print_node_info(node_info n){
    fprintf(stderr,"(%s,%s,%s,((%f,%f) to (%f,%f)))\n",
                n.join_request,
                n.request_propogation,
                n.node_removal,
                n.boundary.from.x,
                n.boundary.from.y,
                n.boundary.to.x,
                n.boundary.to.y
            );
}

static void print_ecosystem(){
    fprintf(stderr,"------------\n");
    fprintf(stderr,"Me:");
    print_node_info(me);
    int i=0;
    fprintf(stderr,"Neighbours list:\n");
    fprintf(stderr,"(Port numbers, boundary)\n");
    for(i=0;i<10;i++){
        if(strcmp(neighbour[i].node_removal,"NULL") || strcmp(neighbour[i].request_propogation,"NULL"))
        {
            fprintf(stderr,"%d\n",i);
            print_node_info(neighbour[i]);
        }
    }
    fprintf(stderr,"------------\n");
}

/* ntokens is overwritten here... shrug.. */
static inline void process_get_command(conn *c, token_t *tokens, size_t ntokens,
		bool return_cas) {
	char *key;
	char key2[1024];
	size_t nkey;
	int i = 0, time;
	item *it;
	token_t *key_token = &tokens[KEY_TOKEN];
	char *suffix;
	assert(c != NULL);
	char buf[1024];//, value[1024];
	int flag;
	int length;
//	char *ptr_to_value;
	it = NULL;

    print_ecosystem();

	do {
		while (key_token->length != 0) {

			key = key_token->value;
			nkey = key_token->length;

			if (nkey > KEY_MAX_LENGTH) {
				out_string(c, "CLIENT_ERROR bad command line format");
				return;
			}
			Point resolved_point = key_point(key);
			if (settings.verbose > 1)
				fprintf(stderr, "Key %s resolves to point  = (%f,%f)\n", key,
						resolved_point.x, resolved_point.y);

            if(mode == NORMAL_NODE){
                if(is_within_boundary(resolved_point,me.boundary)==1){
                    it = item_get(key, nkey);
                }
                else{
                    fprintf(stderr,"Point (%f,%f) is not in zoneboundry([%f,%f],[%f,%f])\n", resolved_point.x,resolved_point.y,me.boundary.from.x,me.boundary.from.y,me.boundary.to.x,me.boundary.to.y);

                    node_info info = get_neighbour_information(key);

                    //adding sleep
                    usleep(1000);


                    request_neighbour(key,buf,"get",&info,NULL);
                    fprintf(stderr, "buf is : %s\n",buf);
                    char *global_data_entry=(char*)pthread_getspecific(global_data_entry_t);
                    fprintf(stderr," value is %s\n",global_data_entry);
                    if(strncmp(buf,"NOT FOUND",9))
                    {
                        deserialize_key_value_str(key2,&flag,&time,&length,buf);
                        fprintf(stderr,"final:%s %d %d %d",key2,flag,time,length);
                        char *flag_as_str = (char*)malloc(sizeof(char)*5);
                        sprintf(flag_as_str,"%d",flag);
                        char *length_as_str = (char*)malloc(sizeof(char)*5);
                        sprintf(length_as_str,"%d",length);
  //                      ptr_to_value=value;
                        add_iov(c, "VALUE ", 6);
                        add_iov(c, key2, strlen(key2));
                        add_iov(c, " ", 1);
                        add_iov(c,  flag_as_str,strlen(flag_as_str));
                        add_iov(c, " ", 1);
                        add_iov(c,  length_as_str, strlen(length_as_str));
                        add_iov(c, "\r\n", 2);
                        pretty_print(global_data_entry,length,"process_get,received_this_value_from_neighbour");
                        add_iov(c, global_data_entry,length);
                        add_iov(c, "\r\n", 2);
                        free(global_data_entry);
                    }
                }
            }
            else
                if( mode == SPLITTING_PARENT_INIT ||
                    mode == SPLITTING_PARENT_MIGRATING ||
                    mode == SPLITTING_CHILD_INIT ||
                    mode == SPLITTING_CHILD_MIGRATING ||
                    mode == MERGING_PARENT_INIT ||
                    mode == MERGING_PARENT_MIGRATING ||
                    mode == MERGING_CHILD_INIT ||
                    mode == MERGING_CHILD_MIGRATING
                    )
            {
                if(is_within_boundary(resolved_point,my_new_boundary)==1){
                    it = item_get(key, nkey);
                }
                else{
                    if(mylist_contains(&trash_both,key)==1)
                    {
                        fprintf(stderr,"key present in trash list, ignoring GETs\n");
                        it=NULL;
                    }
                    else
                    {
                        fprintf(stderr,"Point (%f,%f)\n is not in new zoneboundry([%f,%f],[%f,%f])\n", resolved_point.x,resolved_point.y,my_new_boundary.from.x,my_new_boundary.from.y,my_new_boundary.to.x,my_new_boundary.to.y);

                        it= item_get(key,nkey);
                        if(it==NULL)
                        {
                            node_info info = get_neighbour_information(key);
                            fprintf(stderr,"\n-------info-%s-\n",info.request_propogation);
							request_neighbour(key,buf,"get",&info,NULL);
							fprintf(stderr, "buf is : %s\n",buf);
                            char *global_data_entry=(char*)pthread_getspecific(global_data_entry_t);
							fprintf(stderr," value is %s\n",global_data_entry);
							if(strncmp(buf,"NOT FOUND",9))
							{
								deserialize_key_value_str(key2,&flag,&time,&length,buf);
								char *flag_as_str = (char*)malloc(sizeof(char)*5);
                                sprintf(flag_as_str,"%d",flag);
								char *length_as_str = (char*)malloc(sizeof(char)*5);
                                sprintf(length_as_str,"%d",length);
								fprintf(stderr,"final:%s %d %d %d",key2,flag,time,length);
//								ptr_to_value=value;
								add_iov(c, "VALUE ", 6);
								add_iov(c, key2, strlen(key2));
								add_iov(c, " ", 1);
								add_iov(c,  flag_as_str,strlen(flag_as_str));
								add_iov(c, " ", 1);
								add_iov(c,  length_as_str, strlen(length_as_str));
								add_iov(c, "\r\n", 2);
                                pretty_print(global_data_entry,length,"process_get,received_this_value_from_neighbour");
								add_iov(c, global_data_entry, length);
								add_iov(c, "\r\n", 2);
                                free(global_data_entry);

							}
                        }
                    }
                }
            }

            if (settings.detail_enabled) {
                stats_prefix_record_get(key, nkey, NULL != it);
            }
            if (it) {
                if (i >= c->isize) {
                    item **new_list = realloc(c->ilist, sizeof(item *) * c->isize * 2);
                    if (new_list) {
                        c->isize *= 2;
                        c->ilist = new_list;
                    } else {
                        item_remove(it);
                        break;
                    }
                }

				/*
				 * Construct the response. Each hit adds three elements to the
				 * outgoing data list:
				 *   "VALUE "
				 *   key
				 *   " " + flags + " " + data length + "\r\n" + data (with \r\n)
				 */

				if (return_cas) {
					MEMCACHED_COMMAND_GET(c->sfd, ITEM_key(it), it->nkey,
							it->nbytes, ITEM_get_cas(it));
					/* Goofy mid-flight realloc. */
					if (i >= c->suffixsize) {
						char **new_suffix_list = realloc(c->suffixlist,
								sizeof(char *) * c->suffixsize * 2);
						if (new_suffix_list) {
							c->suffixsize *= 2;
							c->suffixlist = new_suffix_list;
						} else {
							item_remove(it);
							break;
						}
					}

					suffix = cache_alloc(c->thread->suffix_cache);
					if (suffix == NULL ) {
						out_string(c,
								"SERVER_ERROR out of memory making CAS suffix");
						item_remove(it);
						return;
					}
					*(c->suffixlist + i) = suffix;
					int suffix_len = snprintf(suffix, SUFFIX_SIZE, " %llu\r\n",
							(unsigned long long) ITEM_get_cas(it));
					if (add_iov(c, "VALUE ", 6) != 0
							|| add_iov(c, ITEM_key(it), it->nkey) != 0
							|| add_iov(c, ITEM_suffix(it), it->nsuffix - 2) != 0
							|| add_iov(c, suffix, suffix_len) != 0
							|| add_iov(c, ITEM_data(it), it->nbytes) != 0) {
						item_remove(it);
						break;
					}
				} else {
					MEMCACHED_COMMAND_GET(c->sfd, ITEM_key(it), it->nkey,
							it->nbytes, ITEM_get_cas(it));
					if (add_iov(c, "VALUE ", 6) != 0
							|| add_iov(c, ITEM_key(it), it->nkey) != 0
							|| add_iov(c, ITEM_suffix(it),
									it->nsuffix + it->nbytes) != 0) {
						item_remove(it);
						break;
					}
				}


                pretty_print(ITEM_data(it), it->nbytes,"process_get_command,all_cases");
				if (settings.verbose > 1)
					fprintf(stderr, ">%d sending key %s\n", c->sfd,
							ITEM_key(it));

				/* item_get() has incremented it->refcount for us */
				pthread_mutex_lock(&c->thread->stats.mutex);
				c->thread->stats.slab_stats[it->slabs_clsid].get_hits++;
				c->thread->stats.get_cmds++;
				pthread_mutex_unlock(&c->thread->stats.mutex);
				item_update(it);
				*(c->ilist + i) = it;
				i++;

			} else {
				pthread_mutex_lock(&c->thread->stats.mutex);
				c->thread->stats.get_misses++;
				c->thread->stats.get_cmds++;
				pthread_mutex_unlock(&c->thread->stats.mutex);
				MEMCACHED_COMMAND_GET(c->sfd, key, nkey, -1, 0);
			}

			key_token++;
		}

		/*
		 * If the command string hasn't been fully processed, get the next set
		 * of tokens.
		 */
		if (key_token->value != NULL ) {
			ntokens = tokenize_command(key_token->value, tokens, MAX_TOKENS);
			key_token = tokens;
		}

	} while (key_token->value != NULL );

	c->icurr = c->ilist;
	c->ileft = i;
	if (return_cas) {
		c->suffixcurr = c->suffixlist;
		c->suffixleft = i;
	}

	if (settings.verbose > 1)
		fprintf(stderr, ">%d END\n", c->sfd);

	/*
	 If the loop was terminated because of out-of-memory, it is not
	 reliable to add END\r\n to the buffer, because it might not end
	 in \r\n. So we send SERVER_ERROR instead.
	 */
	if (key_token->value != NULL || add_iov(c, "END\r\n", 5) != 0
			|| (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
		out_string(c, "SERVER_ERROR out of memory writing get response");
	} else {
		conn_set_state(c, conn_mwrite);
		c->msgcurr = 0;
	}

	return;
}

static void process_update_command(conn *c, token_t *tokens,
		const size_t ntokens, int comm, bool handle_cas) {
	char *key;
	size_t nkey;
	unsigned int flags;
	int32_t exptime_int = 0;
	time_t exptime;
	int vlen;
	uint64_t req_cas_id = 0;
	item *it;

	assert(c != NULL);

	set_noreply_maybe(c, tokens, ntokens);

	if (tokens[KEY_TOKEN].length > KEY_MAX_LENGTH) {
		out_string(c, "CLIENT_ERROR bad command line format");
		return;
	}

	key = tokens[KEY_TOKEN].value;
	nkey = tokens[KEY_TOKEN].length;

	if (!(safe_strtoul(tokens[2].value, (uint32_t *) &flags)
			&& safe_strtol(tokens[3].value, &exptime_int)
			&& safe_strtol(tokens[4].value, (int32_t *) &vlen))) {
		out_string(c, "CLIENT_ERROR bad command line format");
		return;
	}

	/* Ubuntu 8.04 breaks when I pass exptime to safe_strtol */
	exptime = exptime_int;

	/* Negative exptimes can underflow and end up immortal. realtime() will
	 immediately expire values that are greater than REALTIME_MAXDELTA, but less
	 than process_started, so lets aim for that. */
	if (exptime < 0)
		exptime = REALTIME_MAXDELTA + 1;

	// does cas value exist?
	if (handle_cas) {
		if (!safe_strtoull(tokens[5].value, &req_cas_id)) {
			out_string(c, "CLIENT_ERROR bad command line format");
			return;
		}
	}

	vlen += 2;
	if (vlen < 0 || vlen - 2 < 0) {
		out_string(c, "CLIENT_ERROR bad command line format");
		return;
	}

	if (settings.detail_enabled) {
		stats_prefix_record_set(key, nkey);
	}

    if(mode == NORMAL_NODE){
        Point resolved_point = key_point(key);
        if(settings.verbose > 1)
            fprintf(stderr,"Key %s resolves to point  = (%f,%f)\n", key,resolved_point.x,resolved_point.y);

        if(is_within_boundary(resolved_point,my_new_boundary) == 1){
            pthread_mutex_lock(&list_of_keys_lock);
            mylist_delete(&list_of_keys, key);
            mylist_add(&list_of_keys, key);
            pthread_mutex_unlock(&list_of_keys_lock);
        }
        else {
        // this update command request will be propagated to the right node when the code reaches drive_machine case conn_nread
        }
    }
    else if( mode == SPLITTING_PARENT_INIT ||
        mode == SPLITTING_PARENT_MIGRATING ||
        mode == SPLITTING_CHILD_INIT ||
        mode == SPLITTING_CHILD_MIGRATING ||
        mode == MERGING_PARENT_INIT ||
        mode == MERGING_PARENT_MIGRATING ||
        mode == MERGING_CHILD_INIT ||
        mode == MERGING_CHILD_MIGRATING
    )
    {
        Point resolved_point = key_point(key);
        if(settings.verbose > 1)
        fprintf(stderr,"Key %s resolves to point  = (%f,%f)\n", key,resolved_point.x,resolved_point.y);

        if(is_within_boundary(resolved_point,my_new_boundary) == 1){
            pthread_mutex_lock(&list_of_keys_lock);
            mylist_delete(&list_of_keys, key);
            mylist_add(&list_of_keys, key);
            pthread_mutex_unlock(&list_of_keys_lock);
        }
        else{
            fprintf(stderr,"Point (%f,%f)\n is not in zoneboundry([%f,%f],[%f,%f])\n", resolved_point.x,resolved_point.y,my_new_boundary.from.x,my_new_boundary.from.y,my_new_boundary.to.x,my_new_boundary.to.y);
            //fprintf(stderr,"SIMULATING PUT IGNORE; STORING key in trash_both");
            out_string(c, "STORED");
            mylist_add(&trash_both,key);
        }
    }

    char *key_to_transfer=(char*)malloc(sizeof(char)*1024);
    sprintf(key_to_transfer,"%s",key);
    pthread_setspecific(key_to_transfer_t,key_to_transfer);
    fprintf(stderr,"-------%d------",vlen);
    it = item_alloc(key, nkey, flags, realtime(exptime), vlen);

	if (it == 0) {
		if (!item_size_ok(nkey, flags, vlen))
			out_string(c, "SERVER_ERROR object too large for cache");
		else
			out_string(c, "SERVER_ERROR out of memory storing object");
		/* swallow the data line */
		c->write_and_go = conn_swallow;
		c->sbytes = vlen;

		/* Avoid stale data persisting in cache because we failed alloc.
		 * Unacceptable for SET. Anywhere else too? */
		if (comm == NREAD_SET) {
			it = item_get(key, nkey);
			if (it) {
				item_unlink(it);
				item_remove(it);
			}
		}

		return;
	}
	ITEM_set_cas(it, req_cas_id);

	c->item = it;
	c->ritem = ITEM_data(it);
	c->rlbytes = it->nbytes;
	c->cmd = comm;
	conn_set_state(c, conn_nread);
}

static void process_touch_command(conn *c, token_t *tokens,
		const size_t ntokens) {
	char *key;
	size_t nkey;
	int32_t exptime_int = 0;
	item *it;

	assert(c != NULL);

	set_noreply_maybe(c, tokens, ntokens);

	if (tokens[KEY_TOKEN].length > KEY_MAX_LENGTH) {
		out_string(c, "CLIENT_ERROR bad command line format");
		return;
	}

	key = tokens[KEY_TOKEN].value;
	nkey = tokens[KEY_TOKEN].length;

	pthread_mutex_lock(&list_of_keys_lock);
	mylist_delete(&list_of_keys, key);
	mylist_add(&list_of_keys, key);
	pthread_mutex_unlock(&list_of_keys_lock);

	if (!safe_strtol(tokens[2].value, &exptime_int)) {
		out_string(c, "CLIENT_ERROR invalid exptime argument");
		return;
	}

	it = item_touch(key, nkey, realtime(exptime_int));
	if (it) {
		item_update(it);
		pthread_mutex_lock(&c->thread->stats.mutex);
		c->thread->stats.touch_cmds++;
		c->thread->stats.slab_stats[it->slabs_clsid].touch_hits++;
		pthread_mutex_unlock(&c->thread->stats.mutex);

		out_string(c, "TOUCHED");
		item_remove(it);
	} else {
		pthread_mutex_lock(&c->thread->stats.mutex);
		c->thread->stats.touch_cmds++;
		c->thread->stats.touch_misses++;
		pthread_mutex_unlock(&c->thread->stats.mutex);

		out_string(c, "NOT_FOUND");
	}
}

static void process_arithmetic_command(conn *c, token_t *tokens,
		const size_t ntokens, const bool incr) {
	char temp[INCR_MAX_STORAGE_LEN];
	uint64_t delta;
	char *key;
	size_t nkey;

	assert(c != NULL);

	set_noreply_maybe(c, tokens, ntokens);

	if (tokens[KEY_TOKEN].length > KEY_MAX_LENGTH) {
		out_string(c, "CLIENT_ERROR bad command line format");
		return;
	}

	key = tokens[KEY_TOKEN].value;
	nkey = tokens[KEY_TOKEN].length;

	pthread_mutex_lock(&list_of_keys_lock);
	mylist_delete(&list_of_keys, key);
	mylist_add(&list_of_keys, key);
	pthread_mutex_unlock(&list_of_keys_lock);

	if (!safe_strtoull(tokens[2].value, &delta)) {
		out_string(c, "CLIENT_ERROR invalid numeric delta argument");
		return;
	}

	switch (add_delta(c, key, nkey, incr, delta, temp, NULL )) {
	case OK:
		out_string(c, temp);
		break;
	case NON_NUMERIC:
		out_string(c,
				"CLIENT_ERROR cannot increment or decrement non-numeric value");
		break;
	case EOM:
		out_string(c, "SERVER_ERROR out of memory");
		break;
	case DELTA_ITEM_NOT_FOUND:
		pthread_mutex_lock(&c->thread->stats.mutex);
		if (incr) {
			c->thread->stats.incr_misses++;
		} else {
			c->thread->stats.decr_misses++;
		}
		pthread_mutex_unlock(&c->thread->stats.mutex);

		out_string(c, "NOT_FOUND");
		break;
	case DELTA_ITEM_CAS_MISMATCH:
		break; /* Should never get here */
	}
}

/*
 * adds a delta value to a numeric item.
 *
 * c     connection requesting the operation
 * it    item to adjust
 * incr  true to increment value, false to decrement
 * delta amount to adjust value by
 * buf   buffer for response string
 *
 * returns a response string to send back to the client.
 */
enum delta_result_type do_add_delta(conn *c, const char *key, const size_t nkey,
		const bool incr, const int64_t delta, char *buf, uint64_t *cas,
		const uint32_t hv) {
	char *ptr;
	uint64_t value;
	int res;
	item *it;

	it = do_item_get(key, nkey, hv);
	if (!it) {
		return DELTA_ITEM_NOT_FOUND;
	}

	if (cas != NULL && *cas != 0 && ITEM_get_cas(it) != *cas) {
		do_item_remove(it);
		return DELTA_ITEM_CAS_MISMATCH;
	}

	ptr = ITEM_data(it);

	if (!safe_strtoull(ptr, &value)) {
		do_item_remove(it);
		return NON_NUMERIC;
	}

	if (incr) {
		value += delta;
		MEMCACHED_COMMAND_INCR(c->sfd, ITEM_key(it), it->nkey, value);
	} else {
		if (delta > value) {
			value = 0;
		} else {
			value -= delta;
		}MEMCACHED_COMMAND_DECR(c->sfd, ITEM_key(it), it->nkey, value);
	}

	pthread_mutex_lock(&c->thread->stats.mutex);
	if (incr) {
		c->thread->stats.slab_stats[it->slabs_clsid].incr_hits++;
	} else {
		c->thread->stats.slab_stats[it->slabs_clsid].decr_hits++;
	}
	pthread_mutex_unlock(&c->thread->stats.mutex);

	snprintf(buf, INCR_MAX_STORAGE_LEN, "%llu", (unsigned long long) value);
	res = strlen(buf);
	if (res + 2 > it->nbytes || it->refcount != 1) { /* need to realloc */
		item *new_it;
		new_it = do_item_alloc(ITEM_key(it), it->nkey,
				atoi(ITEM_suffix(it) + 1), it->exptime, res + 2, hv);
		if (new_it == 0) {
			do_item_remove(it);
			return EOM;
		}
		memcpy(ITEM_data(new_it), buf, res);
		memcpy(ITEM_data(new_it) + res, "\r\n", 2);
		item_replace(it, new_it, hv);
		// Overwrite the older item's CAS with our new CAS since we're
		// returning the CAS of the old item below.
		ITEM_set_cas(it, (settings.use_cas) ? ITEM_get_cas(new_it) : 0);
		do_item_remove(new_it); /* release our reference */
	} else { /* replace in-place */
		/* When changing the value without replacing the item, we
		 need to update the CAS on the existing item. */
		mutex_lock(&cache_lock); /* FIXME */
		ITEM_set_cas(it, (settings.use_cas) ? get_cas_id() : 0);
		mutex_unlock(&cache_lock);

		memcpy(ITEM_data(it), buf, res);
		memset(ITEM_data(it) + res, ' ', it->nbytes - res - 2);
		do_item_update(it);
	}

	if (cas) {
		*cas = ITEM_get_cas(it); /* swap the incoming CAS value */
	}
	do_item_remove(it); /* release our reference */
	return OK;
}

static void _normal_delete_operation(conn *c, char* key,size_t nkey){
    item *it;
    pthread_mutex_lock(&list_of_keys_lock);
    mylist_delete(&list_of_keys, key);
    pthread_mutex_unlock(&list_of_keys_lock);

    if (settings.detail_enabled) {
        stats_prefix_record_delete(key, nkey);
    }

    it = item_get(key, nkey);
    if (it) {
        MEMCACHED_COMMAND_DELETE(c->sfd, ITEM_key(it), it->nkey);

        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.slab_stats[it->slabs_clsid].delete_hits++;
        pthread_mutex_unlock(&c->thread->stats.mutex);

        item_unlink(it);
        item_remove(it);      /* release our reference */
        out_string(c, "DELETED");
    } else {
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.delete_misses++;
        pthread_mutex_unlock(&c->thread->stats.mutex);

        out_string(c, "NOT_FOUND");
    }
}

static void process_delete_command(conn *c, token_t *tokens,
		const size_t ntokens) {
	char *key;
	size_t nkey;
	char buf[1024];
	assert(c != NULL);

	if (ntokens > 3) {
		bool hold_is_zero = strcmp(tokens[KEY_TOKEN + 1].value, "0") == 0;
		bool sets_noreply = set_noreply_maybe(c, tokens, ntokens);
		bool valid = (ntokens == 4 && (hold_is_zero || sets_noreply))
				|| (ntokens == 5 && hold_is_zero && sets_noreply);
		if (!valid) {
			out_string(c, "CLIENT_ERROR bad command line format.  "
					"Usage: delete <key> [noreply]");
			return;
		}
	}

	key = tokens[KEY_TOKEN].value;
	nkey = tokens[KEY_TOKEN].length;

	if (nkey > KEY_MAX_LENGTH) {
		out_string(c, "CLIENT_ERROR bad command line format");
		return;
	}
	Point resolved_point = key_point(key);

    if(mode == NORMAL_NODE){
        if(is_within_boundary(resolved_point,me.boundary)==1){
          _normal_delete_operation(c,key,nkey);
        }
        else{
            fprintf(stderr,"Point (%f,%f)\n is not in zoneboundry([%f,%f],[%f,%f])\n", resolved_point.x,resolved_point.y,me.boundary.from.x,me.boundary.from.y,me.boundary.to.x,me.boundary.to.y);
            node_info info = get_neighbour_information(key);
            request_neighbour(key,buf,"delete",&info,NULL);
            out_string(c, "DELETED");
        }
    }
    else
        if( mode == SPLITTING_PARENT_INIT ||
            mode == SPLITTING_PARENT_MIGRATING ||
            mode == SPLITTING_CHILD_INIT ||
            mode == SPLITTING_CHILD_MIGRATING ||
            mode == MERGING_PARENT_INIT ||
            mode == MERGING_PARENT_MIGRATING ||
            mode == MERGING_CHILD_INIT ||
            mode == MERGING_CHILD_MIGRATING
        )
    {
        if(is_within_boundary(resolved_point,my_new_boundary)==1){
            _normal_delete_operation(c,key,nkey);
        }
        else
        {
            fprintf(stderr,"Point (%f,%f)\n is not in zoneboundry([%f,%f],[%f,%f])\n", resolved_point.x,resolved_point.y,my_new_boundary.from.x,my_new_boundary.from.y,my_new_boundary.to.x,my_new_boundary.to.y);
            pthread_mutex_lock(&list_of_keys_lock);
            mylist_delete(&list_of_keys, key);
            mylist_add(&trash_both, key);
            pthread_mutex_unlock(&list_of_keys_lock);

            //out_string(c, "IGNORING DELETE; KEY STORED IN TRASH LIST");
            out_string(c, "DELETED");
        }
    }
}

static void process_verbosity_command(conn *c, token_t *tokens,
		const size_t ntokens) {
	unsigned int level;

	assert(c != NULL);

	set_noreply_maybe(c, tokens, ntokens);

	level = strtoul(tokens[1].value, NULL, 10);
	settings.verbose =
			level > MAX_VERBOSITY_LEVEL ? MAX_VERBOSITY_LEVEL : level;
	out_string(c, "OK");
	return;
}

static void process_slabs_automove_command(conn *c, token_t *tokens,
		const size_t ntokens) {
	unsigned int level;

	assert(c != NULL);

	set_noreply_maybe(c, tokens, ntokens);

	level = strtoul(tokens[2].value, NULL, 10);
	if (level == 0) {
		settings.slab_automove = 0;
	} else if (level == 1 || level == 2) {
		settings.slab_automove = level;
	} else {
		out_string(c, "ERROR");
		return;
	}
	out_string(c, "OK");
	return;
}

static void delete_key_locally(char *key) {
	int nkey = strlen(key);
	item* it = item_get(key, nkey);
	if (it) {
		item_unlink(it);
		item_remove(it);
		pthread_mutex_lock(&list_of_keys_lock);
		mylist_delete(&list_of_keys, key);
		pthread_mutex_unlock(&list_of_keys_lock);
	}
}
static void delete_key_on_child(int child_fd, char *key) {
	//adding sleep before...
	usleep(1000);
	send(child_fd, key, strlen(key), 0);
	usleep(1000);
}

static void receive_and_store_key_value(int sockfd,char* out_param_key,char *out_buf){
    char key[1024];
	int flag1, flag2, flag3, numbytes;

    char buf2[1024];
    memset(buf2, 0, 1024);
    if ((numbytes = recv(sockfd, buf2, sizeof(buf2), 0)) == -1) {
        perror("recv");
        exit(1);
    }
    fprintf(stderr, "received %s\n", buf2);

    deserialize_key_value_str(key, &flag1, &flag2, &flag3, buf2);
    fprintf(stderr, "Client side:%s,%d,%d,%d\n", key, flag1, flag2, flag3);

    item *it;
    it = item_get(key, strlen(key));
    fprintf(stderr,"store_key_value key %s\n",key);
    if (it) {
        item_unlink(it);
        item_remove(it);
    }
    it = item_alloc(key, strlen(key), flag1, realtime(flag2), flag3+2);

    //reads value in binary format from network
    char *ptr = ITEM_data(it);
    char *temp;
    read(sockfd, ptr, flag3);
    temp=ptr+flag3;


  //  ptr=ptr+it->nbytes;
    strcpy(temp,"\r\n");
    item_link(it);
    pretty_print(ITEM_data(it),flag3+2,"receive_and_store_key_value");

    pthread_mutex_lock(&list_of_keys_lock);
    mylist_delete(&list_of_keys, key);
    mylist_add(&list_of_keys, key);
    pthread_mutex_unlock(&list_of_keys_lock);
    if(out_param_key){
        strcpy(out_param_key,key);
    }
    if(out_buf){
        strcpy(out_buf,buf2);
    }
}

static void _receive_keys_and_trash_keys(int sockfd) {
	int MAXDATASIZE = 1024;
	int total_keys_to_be_received = 0;
	char buf[1024],buf2[1024];
	int  numbytes, i = 0;
	memset(buf, '\0', 1024);
	if ((numbytes = recv(sockfd, buf,  sizeof(buf), 0)) == -1) {
		perror("recv");
		exit(1);
	}
	total_keys_to_be_received = atoi(buf);
	fprintf(stderr, "Total keys to be received = %d\n",
			total_keys_to_be_received);

	for (i = 0; i < total_keys_to_be_received; i++) {
	    receive_and_store_key_value(sockfd,NULL,NULL);
	}

	// The following should not be required, but without it, parent didn't receive trash list keys from child when child was departing
	usleep(1000);

	memset(buf, '\0', 1024);
	if ((numbytes = recv(sockfd, buf, MAXDATASIZE - 1, 0)) == -1) {
		perror("recv");
		exit(1);
	}
	total_keys_to_be_received = atoi(buf);
	fprintf(stderr, "Total keys to be deleted = %d\n",
			total_keys_to_be_received);

	for (i = 0; i < total_keys_to_be_received; i++) {
		memset(buf2, '\0', 1024);
		if ((numbytes = recv(sockfd, buf2, sizeof(buf2), 0)) == -1) {
			perror("recv");
			exit(1);
		}
		fprintf(stderr, "Received %s\n", buf2);
		delete_key_locally(buf2);
		fprintf(stderr, "deleting key %s\n", buf2);
	}
}

static void serialize_port_numbers(char *me_request_propogation,
		char *me_node_removal, char*s) {
	sprintf(s, " %s %s ", me_request_propogation, me_node_removal);
}
/*static void deserialize_port_numbers(char *s, neighbour_info *me) {
	char * temp1=NULL,*temp2=NULL;
	sscanf(s, " %s %s %s %s ",temp1,temp2, me->request_propogation,
			me->node_removal);
}*/

static void deserialize_port_numbers2(char *s,char *neighbour_request_propogation,
		char *neighbour_node_removal)
{
	sscanf(s, " %s %s ", neighbour_request_propogation,
				neighbour_node_removal);
}

static void _migrate_key_values(int another_node_fd, my_list keys_to_send) {
	int i = 0;
	char *ptr, buf[1024], key_and_metadata_str[1024];

	fprintf(stderr, "The list of keys to be sent:\n");
	mylist_print(&keys_to_send);

	//v imp sleep
	usleep(100000);
	//usleep(10000*5);
	sprintf(buf, "%d", keys_to_send.size);
	if (send(another_node_fd, buf, strlen(buf), 0) == -1)
		perror("send");

	for (i = 0; i < keys_to_send.size; i++) {
		char *key = keys_to_send.array[i];
		if(mylist_contains(&trash_both,key) != 1){
			//adding one more zero
            usleep(1000000);
            fprintf(stderr,"key to migrate is %s\n",key);
            fprintf(stderr,"length is %d\n",(int)strlen(key));
            item *it = item_get(key, strlen(key));
            ptr = strtok(ITEM_suffix(it), " ");
            fprintf(stderr, "nbytes---%d", it->nbytes-2);
            serialize_key_value_str(key, ptr, it->exptime, it->nbytes-2, key_and_metadata_str);
            fprintf(stderr, "sending key_and_metadata_str %s\n", key_and_metadata_str);
            send(another_node_fd, key_and_metadata_str, strlen(key_and_metadata_str), 0);
            //adding zero
            usleep(100000);
            char *v = ITEM_data(it);
            send(another_node_fd,v,it->nbytes-2,0);
		}
		delete_key_locally(key);
	}
}

static void _trash_keys_in_both_nodes(int child_node_fd, my_list trash_both) {
	int i = 0;
	char buf[1024];
	fprintf(stderr,
			"number of keys to send for deleting is %d\nThe list of keys to be sent for deleting is:\n",
			trash_both.size);
	mylist_print(&trash_both);

	sprintf(buf, "%d", trash_both.size);

	//adding sleep before...
		usleep(100000);

	send(child_node_fd, buf, strlen(buf), 0);

	//adding zeros
	usleep(10000);
	for (i = 0; i < trash_both.size; i++) {
		char *key = trash_both.array[i];
		delete_key_locally(key);
		delete_key_on_child(child_node_fd, key);
	}
	mylist_delete_all(&trash_both);
}

static void* _parent_split_migrate_phase(void *arg){
    int i=0;
    my_list keys_to_send;
    int child_fd = *((int*)(arg));

    mode = SPLITTING_PARENT_MIGRATING;
    fprintf(stderr,"Mode changed: SPLITTING_PARENT_INIT -> SPLITTING_PARENT_MIGRATING\n");

    pthread_mutex_lock(&list_of_keys_lock);
    mylist_init("keys_to_send", &keys_to_send);
    for (i = 0; i < list_of_keys.size; i++) {
        char *key = list_of_keys.array[i];
        Point resolved_point = key_point(key);
        if (is_within_boundary(resolved_point, client_boundary) == 1)
        {
            mylist_add(&keys_to_send, key);
            print_boundaries(client_boundary);
        }
    }
    pthread_mutex_unlock(&list_of_keys_lock);

    fprintf(stderr, "Migrating keys:\n");
    _migrate_key_values(child_fd, keys_to_send);

    fprintf(stderr, "Trashing keys in parent and child:\n");
    _trash_keys_in_both_nodes(child_fd, trash_both);

    close(child_fd); // parent doesn't need this
    me.boundary = my_new_boundary;
    print_all_boundaries();
    mode = NORMAL_NODE;
    fprintf(stderr,"Mode changed: SPLITTING_PARENT_MIGRATING -> NORMAL_NODE\n");
    print_ecosystem();
    return 0;
}

typedef struct tagSplitMigrateKeysArgs{
    int child_fd;
    pthread_key_t *item_lock_type_key;
} split_migrate_key_args;

static void* split_migrate_keys_routine(void *tagArgs){
    split_migrate_key_args *args = tagArgs;
    uint8_t lock_type = ITEM_LOCK_GRANULAR;
    pthread_setspecific(*(args->item_lock_type_key), &lock_type);
    _parent_split_migrate_phase(&args->child_fd);
    return 0;
}

static void getting_key_from_neighbour(char *key, int neighbour_fd) {
	char *ptr;
	item *it=NULL;
	char key_and_metadata_str[1024],buf[1024];
    char key1[1024];
    int flag1, flag2, flag3;
    int i;

	Point resolved_point = key_point(key);
	if(mode == NORMAL_NODE){
	    if(is_within_boundary(resolved_point,me.boundary)==1)
    	    it = item_get(key, strlen(key));
        else{
            fprintf(stderr,"Point (%f,%f)\n is not in zoneboundry([%f,%f],[%f,%f])\n", resolved_point.x,resolved_point.y,me.boundary.from.x,me.boundary.from.y,me.boundary.to.x,me.boundary.to.y);

            node_info info = get_neighbour_information(key);
            request_neighbour(key,buf,"get",&info,NULL);
            fprintf(stderr, "buf is : %s\n",buf);
            if(strncmp(buf,"NOT FOUND",9)==0){
                 it=NULL;
            }
            else{
                deserialize_key_value_str(key1, &flag1, &flag2, &flag3, buf);
                fprintf(stderr, "Client side:%s,%d,%d,%d\n", key1, flag1, flag2, flag3);

                it =  item_get(key1, strlen(key1));
                if (it) {
                    item_unlink(it);
                    item_remove(it);
                }
                it = item_alloc(key1, strlen(key1), flag1, realtime(flag2), flag3+2);

                char *global_data_entry=(char*)pthread_getspecific(global_data_entry_t);

                fprintf(stderr,"Received in global: %s\n",global_data_entry);
                ptr = ITEM_data(it);
                for(i=0;i<flag3;i++){
                    *ptr=global_data_entry[i];
                    fprintf(stderr,"ptr=%c,glob=%c\n",*ptr,global_data_entry[i]);
                    ptr++;
                }
                if(global_data_entry) free(global_data_entry);
                strcpy(ptr,"\r\n");
                fprintf(stderr,"Copied into ptr: %s\n",ptr);
            }
        }
	}
	else
	if( mode == SPLITTING_PARENT_INIT ||
        mode == SPLITTING_PARENT_MIGRATING ||
        mode == SPLITTING_CHILD_INIT ||
        mode == SPLITTING_CHILD_MIGRATING ||
        mode == MERGING_PARENT_INIT ||
        mode == MERGING_PARENT_MIGRATING ||
        mode == MERGING_CHILD_INIT ||
        mode == MERGING_CHILD_MIGRATING
        )
	{
        if(mylist_contains(&trash_both,key)==1) {
            fprintf(stderr,"key present in trash list, ignoring GETs\n");
            it=NULL;
        }
        else it = item_get(key, strlen(key));
	}
	if(it)
	{
		ptr = strtok(ITEM_suffix(it), " ");
		serialize_key_value_str(key, ptr, it->exptime, it->nbytes - 2, key_and_metadata_str);
		fprintf(stderr,"key value str:%s\n", key_and_metadata_str);
		//adding usleep
		usleep(100000);
		send(neighbour_fd, key_and_metadata_str, strlen(key_and_metadata_str), 0);
		//adding zero
		usleep(100000);
        char *v = ITEM_data(it);
        fprintf(stderr,"V is %s\n",v);
        pretty_print(v,it->nbytes-2,"sending_this_value_to_neighbour_when_neighbour_asks_this_key");
        send(neighbour_fd,v,it->nbytes-2,0);
	} else {
		send(neighbour_fd, "NOT FOUND", 9, 0);
	}
}

static void _propagate_update_command_if_required(char *key_to_transfer,char *set_command_to_execute){
    char to_transfer[1024];
    char buf[1024];
    item *it = item_get(key_to_transfer, strlen(key_to_transfer));
    char *ppp = ITEM_data(it);
    pretty_print(ppp,it->nbytes,"just_after_Storing_key_value_locally");

    Point resolved_point = key_point(key_to_transfer);
    if (is_within_boundary(resolved_point, me.boundary) != 1) {
        if(mylist_contains(&trash_both,key_to_transfer)!=1) {
            fprintf(stderr,"storing key %s on neighbour\n",key_to_transfer);
            // the +4 in the next line removes "set " from set_command_to_execute
            sprintf(to_transfer, "%s", (set_command_to_execute+4));
            fprintf(stderr,"set_command_to_execute is %s\n",set_command_to_execute);
            fprintf(stderr,"to_transfer:%s\n",to_transfer);
            node_info info = get_neighbour_information(key_to_transfer);
            request_neighbour(to_transfer, buf, "set",&info,it);
        }
        pthread_mutex_lock(&list_of_keys_lock);
        mylist_delete(&list_of_keys, key_to_transfer);
        pthread_mutex_unlock(&list_of_keys_lock);
        delete_key_locally(key_to_transfer);
        fprintf(stderr,"in _propagate_update_command_if_required, deleted key %s fron this node.\n",key_to_transfer);
    }
    else {
        fprintf(stderr,"storing key %s locally\n",key_to_transfer);
    }
}

static void updating_key_from_neighbour(int new_fd){
	char key[1024];
	char set_command_to_execute_second_half[1024];
    receive_and_store_key_value(new_fd,key,set_command_to_execute_second_half);
    fprintf(stderr,"set_command_to_execute_second_half=%s\n",set_command_to_execute_second_half);

	if(mode == NORMAL_NODE){
        char set_command_to_execute[1024];
	    sprintf(set_command_to_execute,"set %s",set_command_to_execute_second_half);
	    _propagate_update_command_if_required(key,set_command_to_execute);
    }
    else
    if( mode == SPLITTING_PARENT_INIT ||
        mode == SPLITTING_PARENT_MIGRATING ||
        mode == SPLITTING_CHILD_INIT ||
        mode == SPLITTING_CHILD_MIGRATING ||
        mode == MERGING_PARENT_INIT ||
        mode == MERGING_PARENT_MIGRATING ||
        mode == MERGING_CHILD_INIT ||
        mode == MERGING_CHILD_MIGRATING
        )
    {
        if(mylist_contains(&trash_both,key)==1) {
            fprintf(stderr,"key %s present in trash list, ignoring PUTs\n",key);
        }
        else{
            fprintf(stderr,"adding key %s to trash list and ignoring PUT\n",key);
            mylist_add(&trash_both,key);
        }
    }
}

static void deleting_key_from_neighbour(char *key){
    if(mode == NORMAL_NODE){
    	Point resolved_point = key_point(key);
        if(is_within_boundary(resolved_point,me.boundary)==1){
            delete_key_locally(key);
        }
        else{
            fprintf(stderr,"Point (%f,%f)\n is not in zoneboundry([%f,%f],[%f,%f])\n", resolved_point.x,resolved_point.y,me.boundary.from.x,me.boundary.from.y,me.boundary.to.x,me.boundary.to.y);
            node_info info = get_neighbour_information(key);
            char buf[1024];
            request_neighbour(key,buf,"delete",&info,NULL);
        }
    }
    else
    if( mode == SPLITTING_PARENT_INIT ||
        mode == SPLITTING_PARENT_MIGRATING ||
        mode == SPLITTING_CHILD_INIT ||
        mode == SPLITTING_CHILD_MIGRATING ||
        mode == MERGING_PARENT_INIT ||
        mode == MERGING_PARENT_MIGRATING ||
        mode == MERGING_CHILD_INIT ||
        mode == MERGING_CHILD_MIGRATING
        )
    {
        if(mylist_contains(&trash_both,key)==1) {
            fprintf(stderr,"key present in trash list, ignoring DELETE\n");
        }
        else{
            fprintf(stderr,"adding key to trash list and ignoring DELETE\n");
            mylist_add(&trash_both,key);
        }
    }
}

static int is_neighbour_info_not_valid(node_info n){
    return !strcmp(n.node_removal,"NULL") && !strcmp(n.request_propogation,"NULL");
}

static void set_node_info(node_info *n,ZoneBoundary b,char *propagation_port_number,char *removal_port_number){
    n->boundary.from.x=b.from.x;
    n->boundary.from.y=b.from.y;
    n->boundary.to.x=b.to.x;
    n->boundary.to.y=b.to.y;
    sprintf(n->request_propogation,"%s",propagation_port_number);
    sprintf(n->node_removal,"%s",removal_port_number);
}

static void add_to_my_neighbours_list(node_info n) {
    int counter =0;
    for(counter=0;counter<10;counter++)
    {
        if(is_neighbour_info_not_valid(neighbour[counter]))
        {
            set_node_info(&neighbour[counter],n.boundary,n.request_propogation,n.node_removal);
            break;
        }
    }
}

static void copy_node_info(node_info in,node_info *out){
    out->boundary = in.boundary;
    strcpy(out->join_request,in.join_request);
    strcpy(out->request_propogation,in.request_propogation);
    strcpy(out->node_removal,in.node_removal);
}

static void reset_neighbour_entry(int index){
    copy_node_info(NULL_NODE_INFO,&neighbour[index]);
}

static void _update_neighbours_list(char *command, char *propagation_port_number,char *removal_port_number, ZoneBoundary boundary){
    int i=0;
    if(strcmp(command,ADD_NEIGHBOUR_COMMAND)==0){
        for(i =0; i<10; i++){
            if(!is_neighbour_info_not_valid(neighbour[i]) && strcmp(neighbour[i].request_propogation,propagation_port_number)==0){
                // node present already, should have received an update command
                set_node_info(&neighbour[i],boundary,propagation_port_number,removal_port_number);
                break;
            }
            if(is_neighbour_info_not_valid(neighbour[i])){
                set_node_info(&neighbour[i],boundary,propagation_port_number,removal_port_number);
                break;
            }
        }
    }
    else if (strcmp(command,REMOVE_NEIGHBOUR_COMMAND)==0){
        for(i =0; i<10; i++){
            if(!is_neighbour_info_not_valid(neighbour[i]) && strcmp(neighbour[i].request_propogation,propagation_port_number)==0){
                reset_neighbour_entry(i);
                break;
            }
        }
    }
    else if(strcmp(command,UPDATE_NEIGHBOUR_COMMAND)==0){
        for(i =0; i<10; i++){
            if(!is_neighbour_info_not_valid(neighbour[i]) && strcmp(neighbour[i].request_propogation,propagation_port_number)==0){
                set_node_info(&neighbour[i],boundary,propagation_port_number,removal_port_number);
                break;
            }
        }
    }
    else fprintf(stderr,"Invalid neighbour list change command %s\n",command);
}

static void *node_propagation_thread_routine(void *args){
	if(settings.verbose>1)
	        fprintf(stderr,"in node_propagation_thread_routine\n");
	int sockfd, new_fd; // listen on sock_fd, new connection on new_fd
	struct sigaction sa;

	int MAXDATASIZE = 1024;
	char buf[MAXDATASIZE];
	int numbytes;

	int port = find_port(&sockfd);
	sprintf(me.request_propogation,"%d",port);

	if (listen(sockfd, BACKLOG) == -1) {
		perror("listen");
		exit(1);
	}

	sa.sa_handler = sigchld_handler; // reap all dead processes
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = SA_RESTART;

	if (sigaction(SIGCHLD, &sa, NULL ) == -1) {
		perror("sigaction");
		exit(1);
	}
	fprintf(stderr,
			"node_propagation_thread_routine : server: waiting for connections...\n");

	while (1) { // main accept() loop
	    new_fd = receive_connection_from_client(sockfd,"node_propagation_thread_routine");
		memset(buf, '\0', 1024);
		if ((numbytes = recv(new_fd, buf, MAXDATASIZE - 1, 0)) == -1) {
			perror("recv");
			exit(1);
		}

		if (!strcmp(buf, "get")) {
			memset(buf, '\0', 1024);
			if ((numbytes = recv(new_fd, buf, MAXDATASIZE - 1, 0)) == -1) {
				perror("recv");
				exit(1);
			}

			getting_key_from_neighbour(buf, new_fd);
		}
		else if (!strcmp(buf, "set")) {
			    updating_key_from_neighbour(new_fd);
			    //adding usleep
			    usleep(10000);

            if ((numbytes = send(new_fd, "STORED", strlen("STORED"), 0)) == -1) {
                perror("recv");
                exit(1);
            }

        }
        else if(!strcmp(buf,"delete"))
        {
            memset(buf,'\0',1024);
            if ((numbytes = recv(new_fd, buf, MAXDATASIZE-1, 0)) == -1) {
                perror("recv");
                exit(1);
            }

            deleting_key_from_neighbour(buf);
            //adding usleep
            			    usleep(10000);
            if ((numbytes = send(new_fd, "DELETED", strlen("DELETED"), 0)) == -1) {
                perror("recv");
                exit(1);
            }
        }
        else if(!strcmp(buf,ADD_NEIGHBOUR_COMMAND) || !strcmp(buf,REMOVE_NEIGHBOUR_COMMAND) || !strcmp(buf,UPDATE_NEIGHBOUR_COMMAND)){
            char command[1024],propagation_port_number[1024],removal_port_number[1024],boundarystr[1024];
            ZoneBoundary boundary;
            memset(command,'\0',1024);
            memset(propagation_port_number,'\0',1024);
            memset(removal_port_number,'\0',1024);
            memset(boundarystr,'\0',1024);

            fprintf(stderr,"%s command received\n",buf);
            sprintf(command,"%s",buf);

            memset(buf,'\0',1024);
            if ((numbytes = recv(new_fd, buf, MAXDATASIZE-1, 0)) == -1) {
                perror("recv");
                exit(1);
            }
            fprintf(stderr,"Received %s\n",buf);
            deserialize_port_numbers2(buf,propagation_port_number,removal_port_number);

            boundary = *(_recv_boundary_from_neighbour(new_fd));

            _update_neighbours_list(command,propagation_port_number,removal_port_number,boundary);
            print_ecosystem();
        }
        close(new_fd);
    }

    close(sockfd);
	return 0;
}

static ZoneBoundary* _merge_boundaries(ZoneBoundary *a, ZoneBoundary *b) {
	ZoneBoundary *result = (ZoneBoundary *) malloc(sizeof(ZoneBoundary));
	if (a->from.y == b->from.y && a->to.x == b->from.x && a->to.y == b->to.y) {
		result->from.x = a->from.x;
		result->from.y = a->from.y;
		result->to.x = b->to.x;
		result->to.y = b->to.y;
        return result;
	}
	else return _merge_boundaries(b,a);
}

static node_info* get_neighbour_by_boundary(ZoneBoundary *a){
	int counter;
    for(counter=0;counter<10;counter++)
    {
        if(neighbour[counter].boundary.from.x==a->from.x && neighbour[counter].boundary.from.y==a->from.y &&
                neighbour[counter].boundary.to.x==a->to.x && neighbour[counter].boundary.to.y==a->to.y)
            return &neighbour[counter];
    }
    return NULL;
}

static void remove_from_neighbour_list(ZoneBoundary *a){
	int counter;
    for(counter=0;counter<10;counter++)
    {
        if(neighbour[counter].boundary.from.x==a->from.x && neighbour[counter].boundary.from.y==a->from.y &&
                neighbour[counter].boundary.to.x==a->to.x && neighbour[counter].boundary.to.y==a->to.y)
        {
            fprintf(stderr,"\n---removing neighbour from list\n");
            fprintf(stderr,"\n---%f,%f,%f,%f\n",a->from.x ,a->from.y,a->to.x,a->to.y);
            neighbour[counter].boundary.from.x=0;
            neighbour[counter].boundary.from.y=0;
            neighbour[counter].boundary.to.x=0;
            neighbour[counter].boundary.to.y=0;
            strcpy(neighbour[counter].node_removal,"NULL");
            strcpy(neighbour[counter].request_propogation,"NULL");
            break;
        }
        else
            continue;
    }
}

static void serialize_node_info(node_info n,char *buf){
    memset(buf,'\0',1024);
    sprintf(buf,"%s %s (%f,%f) to (%f,%f)",
            n.request_propogation,
            n.node_removal,
            n.boundary.from.x,
            n.boundary.from.y,
            n.boundary.to.x,
            n.boundary.to.y
            );
    fprintf(stderr,"Serialized111: %s\n",buf);
}

static void deserialize_node_info(char *buf, node_info *n){
    memset(n->join_request,'\0',10);
    memset(n->request_propogation,'\0',10);
    memset(n->node_removal,'\0',10);
    sscanf(buf,"%s %s (%f,%f) to (%f,%f)",
                           n->request_propogation,
                           n->node_removal,
                           &n->boundary.from.x,
                           &n->boundary.from.y,
                           &n->boundary.to.x,
                           &n->boundary.to.y);
    char buffer[1024];
    serialize_node_info(*n,buffer);
    fprintf(stderr,"Deserialized111: %s\n",buffer);
}

static int is_same_node_info(node_info n1,node_info n2){
    if(strcmp(n1.node_removal,n2.node_removal) == 0 ) return 1;
    return 0;
}

static int is_neighbour(ZoneBoundary a, ZoneBoundary b){
    if(a.from.x == b.to.x){
        // A is to the right of B in vertical partitioning
        return 1;
    }
    else if(a.to.x == b.from.x){
        // A is to the left of B in vertical partitioning
        return 1;
    }
    else return -1;
}

static void _send_add_remove_update_neighbour_command(char *command,int neighbour_fd,node_info n){
    char buf[1024];
    //adding zero
    usleep(100000);
    fprintf(stderr,"Sending %s\n",command);
    if (send(neighbour_fd,command,strlen(command),0)==-1)
        perror("send");

    //adding zero
    usleep(1000);
    memset(buf,'\0',1024);
    serialize_port_numbers(n.request_propogation, n.node_removal,buf);
    fprintf(stderr,"Sending %s\n",buf);
    if (send(neighbour_fd,buf,strlen(buf),0) == -1)
        perror("send");

    //adding zero
    usleep(1000);
    memset(buf,'\0',1024);
    serialize_boundary(n.boundary,buf);
    fprintf(stderr,"Sending %s\n",buf);
    if (send(neighbour_fd,buf,strlen(buf),0) == -1)
        perror("send");
}

static void _send_remove_neighbour_command(int neighbour_fd,node_info n){
    _send_add_remove_update_neighbour_command(REMOVE_NEIGHBOUR_COMMAND,neighbour_fd,n);
}

static void _send_add_neighbour_command(int neighbour_fd,node_info n){
    _send_add_remove_update_neighbour_command(ADD_NEIGHBOUR_COMMAND,neighbour_fd,n);
}

static void _send_update_neighbour_command(int neighbour_fd,node_info n){
    _send_add_remove_update_neighbour_command(UPDATE_NEIGHBOUR_COMMAND,neighbour_fd,n);
}

static void update_my_neighbours_with_my_info(node_info me,node_info *ignore_node,char *caller) {
    int i=0;
    for(i=0;i<10;i++){
        if(!is_neighbour_info_not_valid(neighbour[i])){
            if(ignore_node && is_same_node_info(neighbour[i],*ignore_node)) continue;
            int neighbour_fd = connect_to("localhost",neighbour[i].request_propogation,caller);
            _send_update_neighbour_command(neighbour_fd,me);
            close(neighbour_fd);
        }
    }
}

static void inform_neighbours_about_new_child(node_info new_node,node_info new_me){
    int counter = 0;
    for(counter = 0;counter < 10; counter++){
        if(!is_neighbour_info_not_valid(neighbour[counter])){
            if(is_neighbour(new_node.boundary,neighbour[counter].boundary)==1){
                // if this neighbour is no longer my neighbour
                int should_reset_this_entry = 0;
                if(is_neighbour(new_me.boundary,neighbour[counter].boundary)!=1){
                    //remove me from neighbour
                    int neighbour_fd = connect_to("localhost",neighbour[counter].request_propogation,"inform_neighbours_about_new_child");
                    fprintf(stderr,"Removing me from neighbour's list via neighbour's port no %s\n",neighbour[counter].request_propogation);
                    _send_remove_neighbour_command(neighbour_fd,new_me);
                    close(neighbour_fd);
                    should_reset_this_entry = 1;
                }
                usleep(1000);
                //if this neighbour is neighbour of new_node
                if(is_neighbour(new_node.boundary,neighbour[counter].boundary)){
                    //add new node to neighbour
                    int neighbour_fd = connect_to("localhost",neighbour[counter].request_propogation,"inform_neighbours_about_new_child");
                    fprintf(stderr,"Removing new node to neighbour's list via neighbour's port no %s\n",neighbour[counter].request_propogation);
                    _send_add_neighbour_command(neighbour_fd,new_node);
                    close(neighbour_fd);
                }
                if(should_reset_this_entry == 1){
                    reset_neighbour_entry(counter);
                }
            }
        }
    }
    update_my_neighbours_with_my_info(me,NULL,"inform_neighbours_about_new_child");
}

static void inform_neighbours_about_dying_child(int dying_child_fd,node_info new_me,node_info dying_child){
    int i=0;
    int MAXDATASIZE = 1024;
    char buf[MAXDATASIZE];
    // dying_child's neighbours
    memset(buf,'\0',1024);
    if (recv(dying_child_fd, buf, MAXDATASIZE-1, 0) == -1) {
        perror("recv");
        exit(1);
    }

    int count_of_valid_entries = atoi(buf);
    fprintf(stderr,"Number of valid node_info in child: %d\n",count_of_valid_entries);
    for(i=0;i<count_of_valid_entries;i++){
        usleep(1000);
        memset(buf,'\0',1024);
        node_info n;
        if (recv(dying_child_fd, buf, MAXDATASIZE-1, 0) == -1) {
            perror("recv");
            exit(1);
        }
        deserialize_node_info(buf,&n);
        if(strncmp(n.node_removal,"NULL",4)!=0){
            if(!is_same_node_info(n,me)){
                fprintf(stderr,"Should process: %s,%s,(%f,%f) to (%f,%f)\n",
                        n.request_propogation,
                        n.node_removal,
                        n.boundary.from.x,
                        n.boundary.from.y,
                        n.boundary.to.x,
                        n.boundary.to.y
                        );
                if(is_neighbour(n.boundary,new_me.boundary)){
                    int neighbour_fd = connect_to("localhost",n.request_propogation,"inform_neighbours_about_dying_child");
                    fprintf(stderr,"Add my new boundary on this neighbour\n");
                    _send_add_neighbour_command(neighbour_fd,new_me);
                    close(neighbour_fd);
                }
                if(is_neighbour(n.boundary,dying_child.boundary)){
                    int neighbour_fd = connect_to("localhost",n.request_propogation,"inform_neighbours_about_dying_child");
                    fprintf(stderr,"Remove dying child boundary on this neighbour\n");
                    _send_remove_neighbour_command(neighbour_fd,dying_child);
                    close(neighbour_fd);
                }
                add_to_my_neighbours_list(n);
            }
        }
    }
    // parent's neighbours
    update_my_neighbours_with_my_info(new_me,&dying_child,"inform_neighbours_about_dying_child");
}

static void *node_removal_listener_thread_routine(void *args) {
	if (settings.verbose > 1)
		fprintf(stderr, "in node_removal_listener_thread_routine\n");

	int sockfd, new_fd; // listen on sock_fd, new connection on new_fd
	struct sigaction sa;
	char buf[1024];

	int port = find_port(&sockfd);
    sprintf(me.node_removal,"%d",port);

	if (listen(sockfd, BACKLOG) == -1) {
		perror("listen");
		exit(1);
	}

	sa.sa_handler = sigchld_handler; // reap all dead processes
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = SA_RESTART;

	if (sigaction(SIGCHLD, &sa, NULL ) == -1) {
		perror("sigaction");
		exit(1);
	}
	fprintf(stderr,
			"node_removal_listener_thread_routine : server: waiting for connections...\n");

	while (1) { // main accept() loop
	    new_fd = receive_connection_from_client(sockfd,"node_removal_listener_thread_routine");

            mode = MERGING_PARENT_INIT;
            fprintf(stderr,"Mode changed: NORMAL_NODE -> MERGING_PARENT_INIT\n");

    		ZoneBoundary *child_boundary = _recv_boundary_from_neighbour(new_fd);
            ZoneBoundary *merged_boundary = _merge_boundaries(&me.boundary,child_boundary);
            node_info dying_child = *(get_neighbour_by_boundary(child_boundary));
            node_info new_me;
            copy_node_info(me,&new_me);
            new_me.boundary = *merged_boundary;
            fprintf(stderr,"my old boundary:");
            print_boundaries(me.boundary);
            fprintf(stderr,"my new boundary:");
            print_boundaries(new_me.boundary);

            ////
            usleep(1000*10);
            serialize_boundary(*merged_boundary,buf);

            send(new_fd,buf,strlen(buf),0);
            ///

            inform_neighbours_about_dying_child(new_fd,new_me,dying_child);
            mode = MERGING_PARENT_MIGRATING;
            fprintf(stderr,"Mode changed: MERGING_PARENT_INIT -> MERGING_PARENT_MIGRATING\n");

            _receive_keys_and_trash_keys(new_fd);
            me.boundary = *merged_boundary;
            my_new_boundary = me.boundary;
            fprintf(stderr,"My new boundary is:\n");
            print_boundaries(me.boundary);
            print_boundaries(my_new_boundary);

            close(new_fd);

            mode = NORMAL_NODE;
            remove_from_neighbour_list(child_boundary);
            fprintf(stderr,"Mode changed: MERGING_PARENT_MIGRATING -> NORMAL_NODE\n");

        }
}



static int send_neighbours_to_child(int new_fd){
	char boundary_str[1024],port_number_str[1024];
	int counter;
	int flag=0;
	for(counter=0;counter<10;counter++)
	{
		if(neighbour[counter].boundary.from.x > my_new_boundary.from.x)
		{
			flag=1;
			fprintf(stderr,"\nsending neighbour boundary from parent to be updated in clients neighbour list:%f,%f\n",neighbour[counter].boundary.from.x , my_new_boundary.from.x);
	        serialize_boundary(neighbour[counter].boundary,boundary_str);

	        //adding usleep
	        usleep(1000);
	        send(new_fd,boundary_str,strlen(boundary_str),0);
	        //adding zero
	        usleep(1000);
	        serialize_port_numbers(neighbour[counter].request_propogation,neighbour[counter].node_removal,port_number_str);
	        send(new_fd,port_number_str,strlen(port_number_str),0);
	        return counter;

		}
	}
	if(flag==0)
	{
	    send(new_fd,"NONE",4,0);
	    return -1;
	}
	return -1;
}

static void receiving_from_parents_parents_neighbours(int new_sockfd){
	char buf[1024],propagation_port_number[1024],removal_port_number[1024];
	int counter;
	ZoneBoundary boundary;

	recv(new_sockfd, buf,1024, 0);
	fprintf(stderr,"receiving from parent1:%s",buf);
	if(strcmp(buf,"NONE"))
	{
		deserialize_boundary(buf,&boundary);

		memset(buf,'\0',1024);
		recv(new_sockfd, buf, 1024, 0);
		fprintf(stderr,"receiving from parent2:%s",buf);
		deserialize_port_numbers2(buf,propagation_port_number,removal_port_number);
		fprintf(stderr,"receiving from parent3:%s,%s",propagation_port_number,removal_port_number);

		for(counter=0;counter<10;counter++)
		{
			if(is_neighbour_info_not_valid(neighbour[counter]))
			{
			    set_node_info(&neighbour[counter],boundary,propagation_port_number,removal_port_number);
				break;
			}
		}
	}
}

static void *join_request_listener_thread_routine(void * args) {
	if (settings.verbose > 1)
		fprintf(stderr, "in join_request_listener_thread_routine ");

	//int entry_to_delete;
	int MAXDATASIZE=1024;
	int sockfd=0,new_fd; // listen on sock_fd, new connection on new_fd
	char buf[1024];
    int numbytes;

	pthread_key_t *item_lock_type_key = (pthread_key_t*)args;
	if(item_lock_type_key) fprintf(stderr,"lock passed on properly\n");
	else {
	    fprintf(stderr,"lock not passed on properly, exiting here\n");
	    exit(-1);
	}
	char neighbour_request_propogation[1024], neighbour_node_removal[1024];
    my_new_boundary = me.boundary;

    fprintf(stderr,"\nin join req....me.joinport:%s\n",me.join_request);
    
    sockfd  = listen_on(me.join_request,"join_request_listener_thread_routine");

    if(sockfd == -1){
        perror("join_req_listener");
        exit(-1);
    }
	while (1) { // main accept() loop
	    new_fd = receive_connection_from_client(sockfd,"join_request_listener_thread_routine");

		mode = SPLITTING_PARENT_INIT;
        fprintf(stderr,"Mode changed: NORMAL_NODE -> SPLITTING_PARENT_INIT\n");

        float x1, y1, x2, y2;

        x1 = my_new_boundary.from.x;
        x2 = my_new_boundary.to.x;
        y1 = my_new_boundary.from.y;
        y2 = my_new_boundary.to.y;

        client_boundary.from.x = x1 + (x2 - x1) / 2;
        client_boundary.from.y = y1;
        client_boundary.to.x = x2;
        client_boundary.to.y = y2;

        my_new_boundary.from.x = x1;
        my_new_boundary.from.y = y1;

        my_new_boundary.to.x = x1 + (x2 - x1) / 2;
        my_new_boundary.to.y = y2;

        if (settings.verbose > 1) {
            fprintf(stderr, "Client boundary");
            print_boundaries(client_boundary);
            fprintf(stderr, "My boundary");
            print_boundaries(me.boundary);
            fprintf(stderr, "My new boundary");
            print_boundaries(my_new_boundary);
        }

        char client_boundary_str[1024];
        char my_new_boundary_str[1024];
        serialize_boundary(client_boundary, client_boundary_str);
        serialize_boundary(my_new_boundary, my_new_boundary_str);

        mylist_init("trash_both",&trash_both);
        //adding usleep
        usleep(1000);
		if (send(new_fd, client_boundary_str, strlen(client_boundary_str), 0) == -1)
			perror("send");

		//adding zero
		usleep(1000);
		if (send(new_fd, my_new_boundary_str, strlen(my_new_boundary_str), 0) == -1)
            perror("send");

		serialize_port_numbers(me.request_propogation, me.node_removal,buf);

		//adding zero
		usleep(1000);
		fprintf(stderr,"\nsending portnumbers:%s\n",buf);

		if (send(new_fd, buf, strlen(buf), 0) == -1)
			perror("send");

		//receiving client port num
	    memset(buf, '\0', 1024);
	    if ((numbytes = recv(new_fd, buf, MAXDATASIZE - 1, 0)) == -1) {
	            perror("recv");
	            exit(1);
	        }

        deserialize_port_numbers2(buf,neighbour_request_propogation,neighbour_node_removal);

        node_info new_node;
        new_node.boundary=client_boundary;
        sprintf(new_node.node_removal,"%s",neighbour_node_removal);
        sprintf(new_node.request_propogation,"%s",neighbour_request_propogation);
        node_info new_me;
        copy_node_info(me,&new_me);
        new_me.boundary = my_new_boundary;

        send_neighbours_to_child(new_fd);
        inform_neighbours_about_new_child(new_node,new_me);

        add_to_my_neighbours_list(new_node);

        usleep(2000);
        pthread_t split_migrate_keys_thread;
        usleep(3000);
        split_migrate_key_args *args=(split_migrate_key_args*)malloc(sizeof(split_migrate_key_args));
        args->child_fd = new_fd;
        args->item_lock_type_key = item_lock_type_key;
        pthread_create(&split_migrate_keys_thread, 0,split_migrate_keys_routine,(void*)args);
        print_ecosystem();
	}
	return 0;
}

static void send_parent_and_my_info_to_bootstrap(char *port_number){
    int sockfd=-1;
    char str[1024],str2[1024];
    char parent_boundary_str[1024];
    
    fprintf(stderr,"\nBootstrap node removal routine is at %s:%s\n","localhost",port_number);
    sockfd= connect_to("localhost", port_number,"send_parent_and_my_info_to_bootstrap");
    
    //sending my boundary
    serialize_boundary(me.boundary,str);
    send(sockfd,str,strlen(str),0);
    usleep(1000);
    
    //sending my join req port number
    sprintf(str2,"%s",me.join_request);
    send(sockfd,str2,strlen(str2),0);
    serialize_boundary(parent, parent_boundary_str);
    
    //sending parent bondary
    usleep(1000);
    send(sockfd,parent_boundary_str,strlen(parent_boundary_str),0);
    
    //sending parent join req port
    usleep(1000);
    send(sockfd,join_server_port_number,strlen(join_server_port_number),0);
    
    close(sockfd);
}


static void *connect_and_split_thread_routine(void *args) {
	int sockfd, numbytes;
	int MAXDATASIZE = 1024;
	char buf[MAXDATASIZE];
	int counter;
	ZoneBoundary neighbour_boundary;

	char neighbour_request_propogation[1024],
    neighbour_node_removal[1024];//, me_request_propogation[1024],
//    me_node_removal[1024];

    sockfd = connect_to(join_server_ip_address, join_server_port_number,"connect_and_split_thread_routine");

	//receiving self boundary
	me.boundary = *(_recv_boundary_from_neighbour(sockfd));
	me.boundary=me.boundary;
	fprintf(stderr, "client's boundary assigned by server\n");

	print_boundaries(me.boundary);

////receiving neighbours boundary
    neighbour_boundary = *(_recv_boundary_from_neighbour(sockfd));

    parent=neighbour_boundary;
    fprintf(stderr, "client received neighbours boundary\n");


	/////////receiving portnumbers
    memset(buf, '\0', 1024);
        if ((numbytes = recv(sockfd, buf, MAXDATASIZE - 1, 0)) == -1) {
            perror("recv");
            exit(1);
        }
        deserialize_port_numbers2(buf,neighbour_request_propogation,neighbour_node_removal);
        fprintf(stderr, "\n Got port numbers: %s %s ", neighbour_request_propogation,
                    neighbour_node_removal);



		for(counter=0;counter<10;counter++)
		{
			if(is_neighbour_info_not_valid(neighbour[counter]))
			{
				neighbour[counter].boundary=neighbour_boundary;
				sprintf(neighbour[counter].node_removal,"%s",neighbour_node_removal);
				sprintf(neighbour[counter].request_propogation,"%s",neighbour_request_propogation);
				break;
			}
			else
				continue;
		}



		memset(buf, '\0', 1024);
		serialize_port_numbers(me.request_propogation, me.node_removal,buf);
		usleep(1000);
		fprintf(stderr,"\nsending client portnumbers:%s\n",buf);
		if (send(sockfd, buf, strlen(buf), 0)== -1)
				perror("send");

		receiving_from_parents_parents_neighbours(sockfd);


    mode = SPLITTING_CHILD_MIGRATING;
    fprintf(stderr,"Mode changed: SPLITTING_CHILD_INIT -> SPLITTING_CHILD_MIGRATING\n");

   _receive_keys_and_trash_keys(sockfd);
    close(sockfd);

    mode = NORMAL_NODE;
    fprintf(stderr,"Mode changed: SPLITTING_CHILD_MIGRATING -> NORMAL_NODE\n");

    send_parent_and_my_info_to_bootstrap("11312");

	pthread_create(&join_request_listening_thread, 0,join_request_listener_thread_routine,args);
	print_ecosystem();
	return 0;
}

static void _send_my_boundary_to(int another_node_fd) {
	char buf[1024];
	serialize_boundary(me.boundary, buf);
	send(another_node_fd, buf, strlen(buf), 0);
}



static void find_smallest_neighbour(node_info *found_neighbour)
{
	int counter;
	float area;
	float min=999999;
	int final_counter=0;
	ZoneBoundary bounds;
	for(counter=0;counter<10;counter++)
		{
		    if(strcmp(neighbour[counter].node_removal,"NULL") || strcmp(neighbour[counter].request_propogation,"NULL"))
            {
                bounds=neighbour[counter].boundary;
                area=calculate_area(bounds);
                if(min>area&&area!=0)
                {
                    min=area;
                    final_counter=counter;
                }
			}
		}

	strcpy(found_neighbour->node_removal, neighbour[final_counter].node_removal);
}

static int count_of_valid_node_info(){
    int i=0;
    int count =0;
    for(i=0;i<10;i++){
        if(!is_neighbour_info_not_valid(neighbour[i])){
            count++;
        }
    }
    return count;
}

static void process_die_command(conn *c) {
	int sockfd=-1, i;
	my_list keys_to_send;
	node_info *found_neighbour;
	char buf[1024];

	found_neighbour = (node_info*) malloc(sizeof(node_info));

	out_string(c,"Die command received, initiating to move all keys to a neighbour\n");
	find_smallest_neighbour(found_neighbour);

	fprintf(stderr,"\nneighbour.node_removal=%s\n",found_neighbour->node_removal);
	sockfd = connect_to("localhost", found_neighbour->node_removal,"process_die_command");
    if(sockfd == -1){
        fprintf(stderr,"Did not connect to neighbour.node_removal port no %s",found_neighbour->node_removal);
        exit(-1);
    }
	fprintf(stderr, "In process_die_command\n");
    mode = MERGING_CHILD_INIT;
    fprintf(stderr, "Mode changed: NORMAL_NODE -> MERGING_CHILD_INIT\n");

   //usleep(1000*1000*5);
	_send_my_boundary_to(sockfd);

    parent = *(_recv_boundary_from_neighbour(sockfd));

    // Send neighbour list to parent.
    int count_of_valid_entries = count_of_valid_node_info();
    memset(buf,'\0',1024);
    sprintf(buf,"%d",count_of_valid_entries);
    send(sockfd,buf,strlen(buf),0);
    fprintf(stderr,"Number of valid node_info: %d\n",count_of_valid_entries);

    for(i=0;i<count_of_valid_entries;i++){
        usleep(1000);
        serialize_node_info(neighbour[i],buf);
        send(sockfd,buf,strlen(buf),0);
    }

    mode = MERGING_CHILD_MIGRATING;
    fprintf(stderr, "Mode changed: MERGING_CHILD_INIT -> MERGING_CHILD_MIGRATING\n");

	pthread_mutex_lock(&list_of_keys_lock);
	mylist_init("keys_to_send",&keys_to_send);
	for (i = 0; i < list_of_keys.size; i++) {
		char *key = list_of_keys.array[i];
		mylist_add(&keys_to_send, key);
	}
	pthread_mutex_unlock(&list_of_keys_lock);

	fprintf(stderr, "Migrating keys to neighbour before shutting down\n");
	_migrate_key_values(sockfd, keys_to_send);

	fprintf(stderr, "Trashing keys in parent and child:\n");
	_trash_keys_in_both_nodes(sockfd, trash_both);

	///
	send_parent_and_my_info_to_bootstrap("11313");
	//
	serialize_boundary(me.boundary,buf);
	out_string(c, "Die command complete\r\n");
	close(sockfd);
	exit(0);
}

static void process_command(conn *c, char *command) {

	token_t tokens[MAX_TOKENS];
	size_t ntokens;
	int comm;

	assert(c != NULL);

	MEMCACHED_PROCESS_COMMAND_START(c->sfd, c->rcurr, c->rbytes);

	if (settings.verbose > 1)
		fprintf(stderr, "<%d %s\n", c->sfd, command);

	/*
	 * for commands set/add/replace, we build an item and read the data
	 * directly into it, then continue in nread_complete().
	 */

	c->msgcurr = 0;
	c->msgused = 0;
	c->iovused = 0;
	if (add_msghdr(c) != 0) {
		out_string(c, "SERVER_ERROR out of memory preparing response");
		return;
	}

	ntokens = tokenize_command(command, tokens, MAX_TOKENS);
	if (ntokens == 2 && (strcmp(tokens[COMMAND_TOKEN].value, "die") == 0)) {
		process_die_command(c);
	} else if (ntokens >= 3
			&& ((strcmp(tokens[COMMAND_TOKEN].value, "get") == 0)
					|| (strcmp(tokens[COMMAND_TOKEN].value, "bget") == 0))) {

		process_get_command(c, tokens, ntokens, false);

	} else if ((ntokens == 6 || ntokens == 7)
			&& ((strcmp(tokens[COMMAND_TOKEN].value, "add") == 0 && (comm =
					NREAD_ADD))
					|| (strcmp(tokens[COMMAND_TOKEN].value, "set") == 0
							&& (comm = NREAD_SET))
					|| (strcmp(tokens[COMMAND_TOKEN].value, "replace") == 0
							&& (comm = NREAD_REPLACE))
					|| (strcmp(tokens[COMMAND_TOKEN].value, "prepend") == 0
							&& (comm = NREAD_PREPEND))
					|| (strcmp(tokens[COMMAND_TOKEN].value, "append") == 0
							&& (comm = NREAD_APPEND)))) {

        if(strcmp(tokens[COMMAND_TOKEN].value, "set") == 0){
            char *set_command_to_execute=(char*)malloc(sizeof(char)*1024);
            sprintf(set_command_to_execute, "%s", command);
            pthread_setspecific(set_command_to_execute_t,set_command_to_execute);
        }

		process_update_command(c, tokens, ntokens, comm, false);

	} else if ((ntokens == 7 || ntokens == 8)
			&& (strcmp(tokens[COMMAND_TOKEN].value, "cas") == 0 && (comm =
					NREAD_CAS))) {

		process_update_command(c, tokens, ntokens, comm, true);

	} else if ((ntokens == 4 || ntokens == 5)
			&& (strcmp(tokens[COMMAND_TOKEN].value, "incr") == 0)) {

		process_arithmetic_command(c, tokens, ntokens, 1);

	} else if (ntokens >= 3
			&& (strcmp(tokens[COMMAND_TOKEN].value, "gets") == 0)) {

		process_get_command(c, tokens, ntokens, true);

	} else if ((ntokens == 4 || ntokens == 5)
			&& (strcmp(tokens[COMMAND_TOKEN].value, "decr") == 0)) {

		process_arithmetic_command(c, tokens, ntokens, 0);

	} else if (ntokens >= 3 && ntokens <= 5
			&& (strcmp(tokens[COMMAND_TOKEN].value, "delete") == 0)) {

		process_delete_command(c, tokens, ntokens);

	} else if ((ntokens == 4 || ntokens == 5)
			&& (strcmp(tokens[COMMAND_TOKEN].value, "touch") == 0)) {

		process_touch_command(c, tokens, ntokens);

	} else if (ntokens >= 2
			&& (strcmp(tokens[COMMAND_TOKEN].value, "stats") == 0)) {

		process_stat(c, tokens, ntokens);

	} else if (ntokens >= 2 && ntokens <= 4
			&& (strcmp(tokens[COMMAND_TOKEN].value, "flush_all") == 0)) {
		time_t exptime = 0;

		pthread_mutex_lock(&list_of_keys_lock);
		mylist_delete_all(&list_of_keys);
		pthread_mutex_unlock(&list_of_keys_lock);

		set_noreply_maybe(c, tokens, ntokens);

		pthread_mutex_lock(&c->thread->stats.mutex);
		c->thread->stats.flush_cmds++;
		pthread_mutex_unlock(&c->thread->stats.mutex);

		if (ntokens == (c->noreply ? 3 : 2)) {
			settings.oldest_live = current_time - 1;
			item_flush_expired();
			out_string(c, "OK");
			return;
		}

		exptime = strtol(tokens[1].value, NULL, 10);
		if (errno == ERANGE) {
			out_string(c, "CLIENT_ERROR bad command line format");
			return;
		}

		/*
		 If exptime is zero realtime() would return zero too, and
		 realtime(exptime) - 1 would overflow to the max unsigned
		 value.  So we process exptime == 0 the same way we do when
		 no delay is given at all.
		 */
		if (exptime > 0)
			settings.oldest_live = realtime(exptime) - 1;
		else
			/* exptime == 0 */
			settings.oldest_live = current_time - 1;
		item_flush_expired();
		out_string(c, "OK");
		return;

	} else if (ntokens == 2
			&& (strcmp(tokens[COMMAND_TOKEN].value, "version") == 0)) {

	out_string(c, "VERSION " VERSION);

} else if (ntokens == 2 && (strcmp(tokens[COMMAND_TOKEN].value, "quit") == 0)) {

	conn_set_state(c, conn_closing);

} else if (ntokens == 2
		&& (strcmp(tokens[COMMAND_TOKEN].value, "shutdown") == 0)) {

	if (settings.shutdown_command) {
		conn_set_state(c, conn_closing);
		raise(SIGINT);
	} else {
		out_string(c, "ERROR: shutdown not enabled");
	}

} else if (ntokens > 1 && strcmp(tokens[COMMAND_TOKEN].value, "slabs") == 0) {
	if (ntokens == 5
			&& strcmp(tokens[COMMAND_TOKEN + 1].value, "reassign") == 0) {
		int src, dst, rv;

		if (settings.slab_reassign == false) {
			out_string(c, "CLIENT_ERROR slab reassignment disabled");
			return;
		}

		src = strtol(tokens[2].value, NULL, 10);
		dst = strtol(tokens[3].value, NULL, 10);

		if (errno == ERANGE) {
			out_string(c, "CLIENT_ERROR bad command line format");
			return;
		}

		rv = slabs_reassign(src, dst);
		switch (rv) {
		case REASSIGN_OK:
			out_string(c, "OK");
			break;
		case REASSIGN_RUNNING:
			out_string(c, "BUSY currently processing reassign request");
			break;
		case REASSIGN_BADCLASS:
			out_string(c, "BADCLASS invalid src or dst class id");
			break;
		case REASSIGN_NOSPARE:
			out_string(c, "NOSPARE source class has no spare pages");
			break;
		case REASSIGN_SRC_DST_SAME:
			out_string(c, "SAME src and dst class are identical");
			break;
		}
		return;
	} else if (ntokens == 4
			&& (strcmp(tokens[COMMAND_TOKEN + 1].value, "automove") == 0)) {
		process_slabs_automove_command(c, tokens, ntokens);
	} else {
		out_string(c, "ERROR");
	}
} else if ((ntokens == 3 || ntokens == 4)
		&& (strcmp(tokens[COMMAND_TOKEN].value, "verbosity") == 0)) {
	process_verbosity_command(c, tokens, ntokens);
} else {
	out_string(c, "ERROR");
}
return;
}

/*
 * if we have a complete line in the buffer, process it.
 */
static int try_read_command(conn *c) {
assert(c != NULL);
assert(c->rcurr <= (c->rbuf + c->rsize));
assert(c->rbytes > 0);

if (c->protocol == negotiating_prot || c->transport == udp_transport) {
	if ((unsigned char) c->rbuf[0] == (unsigned char) PROTOCOL_BINARY_REQ) {
		c->protocol = binary_prot;
	} else {
		c->protocol = ascii_prot;
	}

	if (settings.verbose > 1) {
		fprintf(stderr, "%d: Client using the %s protocol\n", c->sfd,
				prot_text(c->protocol));
	}
}

if (c->protocol == binary_prot) {
	/* Do we have the complete packet header? */
	if (c->rbytes < sizeof(c->binary_header)) {
		/* need more data! */
		return 0;
	} else {
#ifdef NEED_ALIGN
		if (((long)(c->rcurr)) % 8 != 0) {
			/* must realign input buffer */
			memmove(c->rbuf, c->rcurr, c->rbytes);
			c->rcurr = c->rbuf;
			if (settings.verbose > 1) {
				fprintf(stderr, "%d: Realign input buffer\n", c->sfd);
			}
		}
#endif
		protocol_binary_request_header* req;
		req = (protocol_binary_request_header*) c->rcurr;

		if (settings.verbose > 1) {
			/* Dump the packet before we convert it to host order */
			int ii;
			fprintf(stderr, "<%d Read binary protocol data:", c->sfd);
			for (ii = 0; ii < sizeof(req->bytes); ++ii) {
				if (ii % 4 == 0) {
					fprintf(stderr, "\n<%d   ", c->sfd);
				}
				fprintf(stderr, " 0x%02x", req->bytes[ii]);
			}
			fprintf(stderr, "\n");
		}

		c->binary_header = *req;
		c->binary_header.request.keylen = ntohs(req->request.keylen);
		c->binary_header.request.bodylen = ntohl(req->request.bodylen);
		c->binary_header.request.cas = ntohll(req->request.cas);

		if (c->binary_header.request.magic != PROTOCOL_BINARY_REQ) {
			if (settings.verbose) {
				fprintf(stderr, "Invalid magic:  %x\n",
						c->binary_header.request.magic);
			}
			conn_set_state(c, conn_closing);
			return -1;
		}

		c->msgcurr = 0;
		c->msgused = 0;
		c->iovused = 0;
		if (add_msghdr(c) != 0) {
			out_string(c, "SERVER_ERROR out of memory");
			return 0;
		}

		c->cmd = c->binary_header.request.opcode;
		c->keylen = c->binary_header.request.keylen;
		c->opaque = c->binary_header.request.opaque;
		/* clear the returned cas value */
		c->cas = 0;

		dispatch_bin_command(c);

		c->rbytes -= sizeof(c->binary_header);
		c->rcurr += sizeof(c->binary_header);
	}
} else {
	char *el, *cont;

	if (c->rbytes == 0)
		return 0;

	el = memchr(c->rcurr, '\n', c->rbytes);
	if (!el) {
		if (c->rbytes > 1024) {
			/*
			 * We didn't have a '\n' in the first k. This _has_ to be a
			 * large multiget, if not we should just nuke the connection.
			 */
			char *ptr = c->rcurr;
			while (*ptr == ' ') { /* ignore leading whitespaces */
				++ptr;
			}

			if (ptr - c->rcurr > 100
					|| (strncmp(ptr, "get ", 4) && strncmp(ptr, "gets ", 5))) {

				conn_set_state(c, conn_closing);
				return 1;
			}
		}

		return 0;
	}
	cont = el + 1;
	if ((el - c->rcurr) > 1 && *(el - 1) == '\r') {
		el--;
	}
	*el = '\0';

	assert(cont <= (c->rcurr + c->rbytes));

	process_command(c, c->rcurr);

	c->rbytes -= (cont - c->rcurr);
	c->rcurr = cont;

	assert(c->rcurr <= (c->rbuf + c->rsize));
}

return 1;
}

/*
 * read a UDP request.
 */
static enum try_read_result try_read_udp(conn *c) {
int res;

assert(c != NULL);

c->request_addr_size = sizeof(c->request_addr);
res = recvfrom(c->sfd, c->rbuf, c->rsize, 0, &c->request_addr,
		&c->request_addr_size);
if (res > 8) {
	unsigned char *buf = (unsigned char *) c->rbuf;
	pthread_mutex_lock(&c->thread->stats.mutex);
	c->thread->stats.bytes_read += res;
	pthread_mutex_unlock(&c->thread->stats.mutex);

	/* Beginning of UDP packet is the request ID; save it. */
	c->request_id = buf[0] * 256 + buf[1];

	/* If this is a multi-packet request, drop it. */
	if (buf[4] != 0 || buf[5] != 1) {
		out_string(c, "SERVER_ERROR multi-packet request not supported");
		return READ_NO_DATA_RECEIVED;
	}

	/* Don't care about any of the rest of the header. */
	res -= 8;
	memmove(c->rbuf, c->rbuf + 8, res);

	c->rbytes = res;
	c->rcurr = c->rbuf;
	return READ_DATA_RECEIVED;
}
return READ_NO_DATA_RECEIVED;
}

/*
 * read from network as much as we can, handle buffer overflow and connection
 * close.
 * before reading, move the remaining incomplete fragment of a command
 * (if any) to the beginning of the buffer.
 *
 * To protect us from someone flooding a connection with bogus data causing
 * the connection to eat up all available memory, break out and start looking
 * at the data I've got after a number of reallocs...
 *
 * @return enum try_read_result
 */
static enum try_read_result try_read_network(conn *c) {
enum try_read_result gotdata = READ_NO_DATA_RECEIVED;
int res;
int num_allocs = 0;
assert(c != NULL);

if (c->rcurr != c->rbuf) {
	if (c->rbytes != 0) /* otherwise there's nothing to copy */
		memmove(c->rbuf, c->rcurr, c->rbytes);
	c->rcurr = c->rbuf;
}

while (1) {
	if (c->rbytes >= c->rsize) {
		if (num_allocs == 4) {
			return gotdata;
		}
		++num_allocs;
		char *new_rbuf = realloc(c->rbuf, c->rsize * 2);
		if (!new_rbuf) {
			if (settings.verbose > 0)
				fprintf(stderr, "Couldn't realloc input buffer\n");
			c->rbytes = 0; /* ignore what we read */
			out_string(c, "SERVER_ERROR out of memory reading request");
			c->write_and_go = conn_closing;
			return READ_MEMORY_ERROR;
		}
		c->rcurr = c->rbuf = new_rbuf;
		c->rsize *= 2;
	}

	int avail = c->rsize - c->rbytes;
	res = read(c->sfd, c->rbuf + c->rbytes, avail);
	if (res > 0) {
		pthread_mutex_lock(&c->thread->stats.mutex);
		c->thread->stats.bytes_read += res;
		pthread_mutex_unlock(&c->thread->stats.mutex);
		gotdata = READ_DATA_RECEIVED;
		c->rbytes += res;
		if (res == avail) {
			continue;
		} else {
			break;
		}
	}
	if (res == 0) {
		return READ_ERROR;
	}
	if (res == -1) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			break;
		}
		return READ_ERROR;
	}
}
return gotdata;
}

static bool update_event(conn *c, const int new_flags) {
assert(c != NULL);

struct event_base *base = c->event.ev_base;
if (c->ev_flags == new_flags)
	return true;
if (event_del(&c->event) == -1)
	return false;
event_set(&c->event, c->sfd, new_flags, event_handler, (void *) c);
event_base_set(base, &c->event);
c->ev_flags = new_flags;
if (event_add(&c->event, 0) == -1)
	return false;
return true;
}

/*
 * Sets whether we are listening for new connections or not.
 */
void do_accept_new_conns(const bool do_accept) {
conn *next;

for (next = listen_conn; next; next = next->next) {
	if (do_accept) {
		update_event(next, EV_READ | EV_PERSIST);
		if (listen(next->sfd, settings.backlog) != 0) {
			perror("listen");
		}
	} else {
		update_event(next, 0);
		if (listen(next->sfd, 0) != 0) {
			perror("listen");
		}
	}
}

if (do_accept) {
	STATS_LOCK();
	stats.accepting_conns = true;
	STATS_UNLOCK();
} else {
	STATS_LOCK();
	stats.accepting_conns = false;
	stats.listen_disabled_num++;
	STATS_UNLOCK();
	allow_new_conns = false;
	maxconns_handler(-42, 0, 0);
}
}

/*
 * Transmit the next chunk of data from our list of msgbuf structures.
 *
 * Returns:
 *   TRANSMIT_COMPLETE   All done writing.
 *   TRANSMIT_INCOMPLETE More data remaining to write.
 *   TRANSMIT_SOFT_ERROR Can't write any more right now.
 *   TRANSMIT_HARD_ERROR Can't write (c->state is set to conn_closing)
 */
static enum transmit_result transmit(conn *c) {
assert(c != NULL);

if (c->msgcurr < c->msgused && c->msglist[c->msgcurr].msg_iovlen == 0) {
	/* Finished writing the current msg; advance to the next. */
	c->msgcurr++;
}
if (c->msgcurr < c->msgused) {
	ssize_t res;
	struct msghdr *m = &c->msglist[c->msgcurr];

	res = sendmsg(c->sfd, m, 0);
	if (res > 0) {
		pthread_mutex_lock(&c->thread->stats.mutex);
		c->thread->stats.bytes_written += res;
		pthread_mutex_unlock(&c->thread->stats.mutex);

		/* We've written some of the data. Remove the completed
		 iovec entries from the list of pending writes. */
		while (m->msg_iovlen > 0 && res >= m->msg_iov->iov_len) {
			res -= m->msg_iov->iov_len;
			m->msg_iovlen--;
			m->msg_iov++;
		}

		/* Might have written just part of the last iovec entry;
		 adjust it so the next write will do the rest. */
		if (res > 0) {
			m->msg_iov->iov_base = (caddr_t) m->msg_iov->iov_base + res;
			m->msg_iov->iov_len -= res;
		}
		return TRANSMIT_INCOMPLETE;
	}
	if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
		if (!update_event(c, EV_WRITE | EV_PERSIST)) {
			if (settings.verbose > 0)
				fprintf(stderr, "Couldn't update event\n");
			conn_set_state(c, conn_closing);
			return TRANSMIT_HARD_ERROR;
		}
		return TRANSMIT_SOFT_ERROR;
	}
	/* if res == 0 or res == -1 and error is not EAGAIN or EWOULDBLOCK,
	 we have a real error, on which we close the connection */
	if (settings.verbose > 0)
		perror("Failed to write, and not due to blocking");

	if (IS_UDP(c->transport))
		conn_set_state(c, conn_read);
	else
		conn_set_state(c, conn_closing);
	return TRANSMIT_HARD_ERROR;
} else {
	return TRANSMIT_COMPLETE;
}
}

int previous_state=-1;
static void drive_machine(conn *c) {
bool stop = false;
int sfd, flags = 1;
socklen_t addrlen;
struct sockaddr_storage addr;
int nreqs = settings.reqs_per_event;
int res;
const char *str;
char *set_command_to_execute,*key_to_transfer;
// char *ptr;
//item *it;

assert(c != NULL);

while (!stop) {

	switch (c->state) {
	case conn_listening:
		addrlen = sizeof(addr);
		if ((sfd = accept(c->sfd, (struct sockaddr *) &addr, &addrlen)) == -1) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				/* these are transient, so don't log anything */
				stop = true;
			} else if (errno == EMFILE) {
				if (settings.verbose > 0)
					fprintf(stderr, "Too many open connections\n");
				accept_new_conns(false);
				stop = true;
			} else {
				perror("accept()");
				stop = true;
			}
			break;
		}
		if ((flags = fcntl(sfd, F_GETFL, 0)) < 0
				|| fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
			perror("setting O_NONBLOCK");
			close(sfd);
			break;
		}

		if (settings.maxconns_fast
				&& stats.curr_conns + stats.reserved_fds
						>= settings.maxconns - 1) {
			str = "ERROR Too many open connections\r\n";
			res = write(sfd, str, strlen(str));
			close(sfd);
			STATS_LOCK();
			stats.rejected_conns++;
			STATS_UNLOCK();
		} else {
			dispatch_conn_new(sfd, conn_new_cmd, EV_READ | EV_PERSIST,
					DATA_BUFFER_SIZE, tcp_transport);
		}

		stop = true;
		break;

	case conn_waiting:
		if (!update_event(c, EV_READ | EV_PERSIST)) {
			if (settings.verbose > 0)
				fprintf(stderr, "Couldn't update event\n");
			conn_set_state(c, conn_closing);
			break;
		}

		conn_set_state(c, conn_read);
		stop = true;
		break;

	case conn_read:
		res = IS_UDP(c->transport) ? try_read_udp(c) : try_read_network(c);

		switch (res) {
		case READ_NO_DATA_RECEIVED:
			conn_set_state(c, conn_waiting);
			break;
		case READ_DATA_RECEIVED:
			conn_set_state(c, conn_parse_cmd);
			break;
		case READ_ERROR:
			conn_set_state(c, conn_closing);
			break;
		case READ_MEMORY_ERROR: /* Failed to allocate more memory */
			/* State already set by try_read_network */
			break;
		}
		break;

	case conn_parse_cmd:
		if (try_read_command(c) == 0) {
			/* wee need more data! */
			conn_set_state(c, conn_waiting);
		}

		break;

	case conn_new_cmd:
		/* Only process nreqs at a time to avoid starving other
		 connections */

		--nreqs;
		if (nreqs >= 0) {
			reset_cmd_handler(c);
		} else {
			pthread_mutex_lock(&c->thread->stats.mutex);
			c->thread->stats.conn_yields++;
			pthread_mutex_unlock(&c->thread->stats.mutex);
			if (c->rbytes > 0) {
				/* We have already read in data into the input buffer,
				 so libevent will most likely not signal read events
				 on the socket (unless more data is available. As a
				 hack we should just put in a request to write data,
				 because that should be possible ;-)
				 */
				if (!update_event(c, EV_WRITE | EV_PERSIST)) {
					if (settings.verbose > 0)
						fprintf(stderr, "Couldn't update event\n");
					conn_set_state(c, conn_closing);
				}
			}
			stop = true;
		}
		break;

	case conn_nread:
	    previous_state = conn_nread;
        key_to_transfer=(char*)pthread_getspecific(key_to_transfer_t);
        fprintf(stderr,"1.storing key %s\n",key_to_transfer);
		if (c->rlbytes == 0) {
			complete_nread(c);
			break;
		}
        fprintf(stderr,"2.storing key %s\n",key_to_transfer);

		/* first check if we have leftovers in the conn_read buffer */
		if (c->rbytes > 0) {
			int tocopy = c->rbytes > c->rlbytes ? c->rlbytes : c->rbytes;
			if (c->ritem != c->rcurr) {
				memmove(c->ritem, c->rcurr, tocopy);
			}
			c->ritem += tocopy;
			c->rlbytes -= tocopy;
			c->rcurr += tocopy;
			c->rbytes -= tocopy;
			if (c->rlbytes == 0) {
				break;
			}
		}
        fprintf(stderr,"3.storing key %s\n",key_to_transfer);
		/*now try reading from the socket*/
		res = read(c->sfd, c->ritem, c->rlbytes);

		if (res > 0) {
			pthread_mutex_lock(&c->thread->stats.mutex);
			c->thread->stats.bytes_read += res;
			pthread_mutex_unlock(&c->thread->stats.mutex);
			if (c->rcurr == c->ritem) {
				c->rcurr += res;
			}
			c->ritem += res;
			c->rlbytes -= res;
			break;
		}
        fprintf(stderr,"5.storing key %s\n",key_to_transfer);

		if (res == 0) { /* end of stream */
			conn_set_state(c, conn_closing);
			break;
		}
        fprintf(stderr,"6.storing key %s\n",key_to_transfer);
		if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
			if (!update_event(c, EV_READ | EV_PERSIST)) {
				if (settings.verbose > 0)
					fprintf(stderr, "Couldn't update event\n");
				conn_set_state(c, conn_closing);
				break;
			}
			stop = true;
			break;
		}
        fprintf(stderr,"7.storing key %s\n",key_to_transfer);
		/* otherwise we have a real error, on which we close the connection */
		if (settings.verbose > 0) {
			fprintf(stderr, "Failed to read, and not due to blocking:\n"
					"errno: %d %s \n"
					"rcurr=%lx ritem=%lx rbuf=%lx rlbytes=%d rsize=%d\n", errno,
					strerror(errno), (long) c->rcurr, (long) c->ritem,
					(long) c->rbuf, (int) c->rlbytes, (int) c->rsize);
		}
		conn_set_state(c, conn_closing);
		break;

	case conn_swallow:
		/* we are reading sbytes and throwing them away */
		if (c->sbytes == 0) {
			conn_set_state(c, conn_new_cmd);
			break;
		}

		/* first check if we have leftovers in the conn_read buffer */
		if (c->rbytes > 0) {
			int tocopy = c->rbytes > c->sbytes ? c->sbytes : c->rbytes;
			c->sbytes -= tocopy;
			c->rcurr += tocopy;
			c->rbytes -= tocopy;
			break;
		}

		/*  now try reading from the socket */
		res = read(c->sfd, c->rbuf,
				c->rsize > c->sbytes ? c->sbytes : c->rsize);
		if (res > 0) {
			pthread_mutex_lock(&c->thread->stats.mutex);
			c->thread->stats.bytes_read += res;
			pthread_mutex_unlock(&c->thread->stats.mutex);
			c->sbytes -= res;
			break;
		}
		if (res == 0) { /* end of stream */
			conn_set_state(c, conn_closing);
			break;
		}
		if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
			if (!update_event(c, EV_READ | EV_PERSIST)) {
				if (settings.verbose > 0)
					fprintf(stderr, "Couldn't update event\n");
				conn_set_state(c, conn_closing);
				break;
			}
			stop = true;
			break;
		}
		/* otherwise we have a real error, on which we close the connection */
		if (settings.verbose > 0)
			fprintf(stderr, "Failed to read, and not due to blocking\n");
		conn_set_state(c, conn_closing);
		break;

	case conn_write:
	    set_command_to_execute=(char*)pthread_getspecific(set_command_to_execute_t);
	    if(previous_state == conn_nread && set_command_to_execute && strncmp(set_command_to_execute,"set ",4)==0){
            key_to_transfer=(char*)pthread_getspecific(key_to_transfer_t);
            _propagate_update_command_if_required(key_to_transfer,set_command_to_execute);
            free(set_command_to_execute);
            free(key_to_transfer);
            previous_state = -1;
        }
        /*
		 * We want to write out a simple response. If we haven't already,
		 * assemble it into a msgbuf list (this will be a single-entry
		 * list for TCP or a two-entry list for UDP).
		 */
		if (c->iovused == 0 || (IS_UDP(c->transport) && c->iovused == 1)) {
			if (add_iov(c, c->wcurr, c->wbytes) != 0) {
				if (settings.verbose > 0)
					fprintf(stderr, "Couldn't build response\n");
				conn_set_state(c, conn_closing);
				break;
			}
		}

		/* fall through... */

	case conn_mwrite:
		if (IS_UDP(c->transport) && c->msgcurr == 0
				&& build_udp_headers(c) != 0) {
			if (settings.verbose > 0)
				fprintf(stderr, "Failed to build UDP headers\n");
			conn_set_state(c, conn_closing);
			break;
		}
		switch (transmit(c)) {
		case TRANSMIT_COMPLETE:
			if (c->state == conn_mwrite) {
				while (c->ileft > 0) {
					item *it = *(c->icurr);
					assert((it->it_flags & ITEM_SLABBED) == 0);
					item_remove(it);
					c->icurr++;
					c->ileft--;
				}
				while (c->suffixleft > 0) {
					char *suffix = *(c->suffixcurr);
					cache_free(c->thread->suffix_cache, suffix);
					c->suffixcurr++;
					c->suffixleft--;
				}
				/* XXX:  I don't know why this wasn't the general case */
				if (c->protocol == binary_prot) {
					conn_set_state(c, c->write_and_go);
				} else {
					conn_set_state(c, conn_new_cmd);
				}
			} else if (c->state == conn_write) {
				if (c->write_and_free) {
					free(c->write_and_free);
					c->write_and_free = 0;
				}
				conn_set_state(c, c->write_and_go);
			} else {
				if (settings.verbose > 0)
					fprintf(stderr, "Unexpected state %d\n", c->state);
				conn_set_state(c, conn_closing);
			}
			break;

		case TRANSMIT_INCOMPLETE:
		case TRANSMIT_HARD_ERROR:
			break; /* Continue in state machine. */

		case TRANSMIT_SOFT_ERROR:
			stop = true;
			break;
		}
		break;

	case conn_closing:
		if (IS_UDP(c->transport))
			conn_cleanup(c);
		else
			conn_close(c);
		stop = true;
		break;

	case conn_max_state:
		assert(false);
		break;
	}
}

return;
}

void event_handler(const int fd, const short which, void *arg) {
conn *c;

c = (conn *) arg;
assert(c != NULL);

c->which = which;

/* sanity */
if (fd != c->sfd) {
	if (settings.verbose > 0)
		fprintf(stderr, "Catastrophic: event fd doesn't match conn fd!\n");
	conn_close(c);
	return;
}

drive_machine(c);

/* wait for next event */
return;
}

static int new_socket(struct addrinfo *ai) {
int sfd;
int flags;

if ((sfd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol)) == -1) {
	return -1;
}

if ((flags = fcntl(sfd, F_GETFL, 0)) < 0
		|| fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
	perror("setting O_NONBLOCK");
	close(sfd);
	return -1;
}
return sfd;
}

/*
 * Sets a socket's send buffer size to the maximum allowed by the system.
 */
static void maximize_sndbuf(const int sfd) {
socklen_t intsize = sizeof(int);
int last_good = 0;
int min, max, avg;
int old_size;

/* Start with the default size. */
if (getsockopt(sfd, SOL_SOCKET, SO_SNDBUF, &old_size, &intsize) != 0) {
	if (settings.verbose > 0)
		perror("getsockopt(SO_SNDBUF)");
	return;
}

/* Binary-search for the real maximum. */
min = old_size;
max = MAX_SENDBUF_SIZE;

while (min <= max) {
	avg = ((unsigned int) (min + max)) / 2;
	if (setsockopt(sfd, SOL_SOCKET, SO_SNDBUF, (void *) &avg, intsize) == 0) {
		last_good = avg;
		min = avg + 1;
	} else {
		max = avg - 1;
	}
}

if (settings.verbose > 1)
	fprintf(stderr, "<%d send buffer was %d, now %d\n", sfd, old_size,
			last_good);
}

/**
 * Create a socket and bind it to a specific port number
 * @param interface the interface to bind to
 * @param port the port number to bind to
 * @param transport the transport protocol (TCP / UDP)
 * @param portnumber_file A filepointer to write the port numbers to
 *        when they are successfully added to the list of ports we
 *        listen on.
 */
static int server_socket(const char *interface, int port,
	enum network_transport transport, FILE *portnumber_file) {
int sfd;
struct linger ling = { 0, 0 };
struct addrinfo *ai;
struct addrinfo *next;
struct addrinfo hints = { .ai_flags = AI_PASSIVE, .ai_family = AF_UNSPEC };
char port_buf[NI_MAXSERV];
int error;
int success = 0;
int flags = 1;

hints.ai_socktype = IS_UDP(transport) ? SOCK_DGRAM : SOCK_STREAM;

if (port == -1) {
	port = 0;
}
snprintf(port_buf, sizeof(port_buf), "%d", port);
error = getaddrinfo(interface, port_buf, &hints, &ai);
if (error != 0) {
	if (error != EAI_SYSTEM)
		fprintf(stderr, "getaddrinfo(): %s\n", gai_strerror(error));
	else
		perror("getaddrinfo()");
	return 1;
}

for (next = ai; next; next = next->ai_next) {
	conn *listen_conn_add;
	if ((sfd = new_socket(next)) == -1) {
		/* getaddrinfo can return "junk" addresses,
		 * we make sure at least one works before erroring.
		 */
		if (errno == EMFILE) {
			/* ...unless we're out of fds */
			perror("server_socket");
			exit(EX_OSERR);
		}
		continue;
	}

#ifdef IPV6_V6ONLY
	if (next->ai_family == AF_INET6) {
		error = setsockopt(sfd, IPPROTO_IPV6, IPV6_V6ONLY, (char *) &flags,
				sizeof(flags));
		if (error != 0) {
			perror("setsockopt");
			close(sfd);
			continue;
		}
	}
#endif

	setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (void *) &flags, sizeof(flags));
	if (IS_UDP(transport)) {
		maximize_sndbuf(sfd);
	} else {
		error = setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, (void *) &flags,
				sizeof(flags));
		if (error != 0)
			perror("setsockopt");

		error = setsockopt(sfd, SOL_SOCKET, SO_LINGER, (void *) &ling,
				sizeof(ling));
		if (error != 0)
			perror("setsockopt");

		error = setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, (void *) &flags,
				sizeof(flags));
		if (error != 0)
			perror("setsockopt");
	}

	if (bind(sfd, next->ai_addr, next->ai_addrlen) == -1) {
		if (errno != EADDRINUSE) {
			perror("bind()");
			close(sfd);
			freeaddrinfo(ai);
			return 1;
		}
		close(sfd);
		continue;
	} else {
		success++;
		if (!IS_UDP(transport) && listen(sfd, settings.backlog) == -1) {
			perror("listen()");
			close(sfd);
			freeaddrinfo(ai);
			return 1;
		}
		if (portnumber_file != NULL
				&& (next->ai_addr->sa_family == AF_INET
						|| next->ai_addr->sa_family == AF_INET6)) {
			union {
				struct sockaddr_in in;
				struct sockaddr_in6 in6;
			} my_sockaddr;
			socklen_t len = sizeof(my_sockaddr);
			if (getsockname(sfd, (struct sockaddr*) &my_sockaddr, &len) == 0) {
				if (next->ai_addr->sa_family == AF_INET) {
					fprintf(portnumber_file, "%s INET: %u\n",
							IS_UDP(transport) ? "UDP" : "TCP",
							ntohs(my_sockaddr.in.sin_port));
				} else {
					fprintf(portnumber_file, "%s INET6: %u\n",
							IS_UDP(transport) ? "UDP" : "TCP",
							ntohs(my_sockaddr.in6.sin6_port));
				}
			}
		}
	}

	if (IS_UDP(transport)) {
		int c;

		for (c = 0; c < settings.num_threads_per_udp; c++) {
			/* this is guaranteed to hit all threads because we round-robin */
			dispatch_conn_new(sfd, conn_read, EV_READ | EV_PERSIST,
					UDP_READ_BUFFER_SIZE, transport);
		}
	} else {
		if (!(listen_conn_add = conn_new(sfd, conn_listening,
				EV_READ | EV_PERSIST, 1, transport, main_base))) {
			fprintf(stderr, "failed to create listening connection\n");
			exit(EXIT_FAILURE);
		}
		listen_conn_add->next = listen_conn;
		listen_conn = listen_conn_add;
	}
}

freeaddrinfo(ai);

/* Return zero iff we detected no errors in starting up connections */
return success == 0;
}

static int server_sockets(int port, enum network_transport transport,
	FILE *portnumber_file) {
if (settings.inter == NULL ) {
	return server_socket(settings.inter, port, transport, portnumber_file);
} else {
// tokenize them and bind to each one of them..
	char *b;
	int ret = 0;
	char *list = strdup(settings.inter);

	if (list == NULL ) {
		fprintf(stderr,
				"Failed to allocate memory for parsing server interface string\n");
		return 1;
	}
	for (char *p = strtok_r(list, ";,", &b); p != NULL ;
			p = strtok_r(NULL, ";,", &b)) {
		int the_port = port;
		char *s = strchr(p, ':');
		if (s != NULL ) {
			*s = '\0';
			++s;
			if (!safe_strtol(s, &the_port)) {
				fprintf(stderr, "Invalid port number: \"%s\"", s);
				return 1;
			}
		}
		if (strcmp(p, "*") == 0) {
			p = NULL;
		}
		ret |= server_socket(p, the_port, transport, portnumber_file);
	}
	free(list);
	return ret;
}
}

static int new_socket_unix(void) {
int sfd;
int flags;

if ((sfd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
	perror("socket()");
	return -1;
}

if ((flags = fcntl(sfd, F_GETFL, 0)) < 0
		|| fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
	perror("setting O_NONBLOCK");
	close(sfd);
	return -1;
}
return sfd;
}

static int server_socket_unix(const char *path, int access_mask) {
int sfd;
struct linger ling = { 0, 0 };
struct sockaddr_un addr;
struct stat tstat;
int flags = 1;
int old_umask;

if (!path) {
	return 1;
}

if ((sfd = new_socket_unix()) == -1) {
	return 1;
}

/*
 * Clean up a previous socket file if we left it around
 */
if (lstat(path, &tstat) == 0) {
	if (S_ISSOCK(tstat.st_mode))
		unlink(path);
}

setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (void *) &flags, sizeof(flags));
setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, (void *) &flags, sizeof(flags));
setsockopt(sfd, SOL_SOCKET, SO_LINGER, (void *) &ling, sizeof(ling));

/*
 * the memset call clears nonstandard fields in some impementations
 * that otherwise mess things up.
 */
memset(&addr, 0, sizeof(addr));

addr.sun_family = AF_UNIX;
strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);
assert(strcmp(addr.sun_path, path) == 0);
old_umask = umask(~(access_mask & 0777));
if (bind(sfd, (struct sockaddr *) &addr, sizeof(addr)) == -1) {
	perror("bind()");
	close(sfd);
	umask(old_umask);
	return 1;
}
umask(old_umask);
if (listen(sfd, settings.backlog) == -1) {
	perror("listen()");
	close(sfd);
	return 1;
}
if (!(listen_conn = conn_new(sfd, conn_listening, EV_READ | EV_PERSIST, 1,
		local_transport, main_base))) {
	fprintf(stderr, "failed to create listening connection\n");
	exit(EXIT_FAILURE);
}

return 0;
}

/*
 * We keep the current time of day in a global variable that's updated by a
 * timer event. This saves us a bunch of time() system calls (we really only
 * need to get the time once a second, whereas there can be tens of thousands
 * of requests a second) and allows us to use server-start-relative timestamps
 * rather than absolute UNIX timestamps, a space savings on systems where
 * sizeof(time_t) > sizeof(unsigned int).
 */
volatile rel_time_t current_time;
static struct event clockevent;

/* libevent uses a monotonic clock when available for event scheduling. Aside
 * from jitter, simply ticking our internal timer here is accurate enough.
 * Note that users who are setting explicit dates for expiration times *must*
 * ensure their clocks are correct before starting memcached. */
static void clock_handler(const int fd, const short which, void *arg) {
struct timeval t = { .tv_sec = 1, .tv_usec = 0 };
static bool initialized = false;
#if defined(HAVE_CLOCK_GETTIME) && defined(CLOCK_MONOTONIC)
static bool monotonic = false;
static time_t monotonic_start;
#endif

if (initialized) {
	/* only delete the event if it's actually there. */
	evtimer_del(&clockevent);
} else {
	initialized = true;
	/* process_started is initialized to time() - 2. We initialize to 1 so
	 * flush_all won't underflow during tests. */
#if defined(HAVE_CLOCK_GETTIME) && defined(CLOCK_MONOTONIC)
	struct timespec ts;
	if (clock_gettime(CLOCK_MONOTONIC, &ts) == 0) {
		monotonic = true;
		monotonic_start = ts.tv_sec - 2;
	}
#endif
}

evtimer_set(&clockevent, clock_handler, 0);
event_base_set(main_base, &clockevent);
evtimer_add(&clockevent, &t);

#if defined(HAVE_CLOCK_GETTIME) && defined(CLOCK_MONOTONIC)
if (monotonic) {
	struct timespec ts;
	if (clock_gettime(CLOCK_MONOTONIC, &ts) == -1)
	return;
	current_time = (rel_time_t) (ts.tv_sec - monotonic_start);
	return;
}
#endif
{
	struct timeval tv;
	gettimeofday(&tv, NULL );
	current_time = (rel_time_t) (tv.tv_sec - process_started);
}
}

static void usage(void) {
printf(PACKAGE " " VERSION "\n");
printf(
		"-p <num>      TCP port number to listen on (default: 11211)\n"
				"-U <num>      UDP port number to listen on (default: 11211, 0 is off)\n"
				"-s <file>     UNIX socket path to listen on (disables network support)\n"
				"-x <num>      Lower boundary x coordinate\n"
				"-y <num>      Lower boundary y coordinate\n"
				"-X <num>      Upper boundary x coordinate\n"
				"-Y <num>      Upper boundary y coordinate\n"
				"-A            enable ascii \"shutdown\" command\n"
				"-a <mask>     access mask for UNIX socket, in octal (default: 0700)\n"
				"-l <addr>     interface to listen on (default: INADDR_ANY, all addresses)\n"
				"              <addr> may be specified as host:port. If you don't specify\n"
				"              a port number, the value you specified with -p or -U is\n"
				"              used. You may specify multiple addresses separated by comma\n"
				"              or by using -l multiple times\n"

				"-d            run as a daemon\n"
				"-r            maximize core file limit\n"
				"-u <username> assume identity of <username> (only when run as root)\n"
				"-m <num>      max memory to use for items in megabytes (default: 64 MB)\n"
				"-M            return error on memory exhausted (rather than removing items)\n"
				"-c <num>      max simultaneous connections (default: 1024)\n"
				"-k            lock down all paged memory.  Note that there is a\n"
				"              limit on how much memory you may lock.  Trying to\n"
				"              allocate more than that would fail, so be sure you\n"
				"              set the limit correctly for the user you started\n"
				"              the daemon with (not for -u <username> user;\n"
				"              under sh this is done with 'ulimit -S -l NUM_KB').\n"
				"-v            verbose (print errors/warnings while in event loop)\n"
				"-vv           very verbose (also print client commands/reponses)\n"
				"-vvv          extremely verbose (also print internal state transitions)\n"
				"-h            print this help and exit\n"
				"-i            print memcached and libevent license\n"
				"-P <file>     save PID in <file>, only used with -d option\n"
				"-f <factor>   chunk size growth factor (default: 1.25)\n"
				"-n <bytes>    minimum space allocated for key+value+flags (default: 48)\n");
printf(
		"-L            Try to use large memory pages (if available). Increasing\n"
				"              the memory page size could reduce the number of TLB misses\n"
				"              and improve the performance. In order to get large pages\n"
				"              from the OS, memcached will allocate the total item-cache\n"
				"              in one large chunk.\n");
printf(
		"-D <char>     Use <char> as the delimiter between key prefixes and IDs.\n"
				"              This is used for per-prefix stats reporting. The default is\n"
				"              \":\" (colon). If this option is specified, stats collection\n"
				"              is turned on automatically; if not, then it may be turned on\n"
				"              by sending the \"stats detail on\" command to the server.\n");
printf("-t <num>      number of threads to use (default: 4)\n");
printf(
		"-R            Maximum number of requests per event, limits the number of\n"
				"              requests process for a given connection to prevent \n"
				"              starvation (default: 20)\n");
printf("-C            Disable use of CAS\n");
printf("-b            Set the backlog queue limit (default: 1024)\n");
printf(
		"-B            Binding protocol - one of ascii, binary, or auto (default)\n");
printf(
		"-I            Override the size of each slab page. Adjusts max item size\n"
				"              (default: 1mb, min: 1k, max: 128m)\n");
#ifdef ENABLE_SASL
printf("-S            Turn on Sasl authentication\n");
#endif
printf(
		"-o            Comma separated list of extended or experimental options\n"
				"              - (EXPERIMENTAL) maxconns_fast: immediately close new\n"
				"                connections if over maxconns limit\n"
				"              - hashpower: An integer multiplier for how large the hash\n"
				"                table should be. Can be grown at runtime if not big enough.\n"
				"                Set this based on \"STAT hash_power_level\" before a \n"
				"                restart.\n");
return;
}

static void usage_license(void) {
printf(PACKAGE " " VERSION "\n\n");
printf(
		"Copyright (c) 2003, Danga Interactive, Inc. <http://www.danga.com/>\n"
				"All rights reserved.\n"
				"\n"
				"Redistribution and use in source and binary forms, with or without\n"
				"modification, are permitted provided that the following conditions are\n"
				"met:\n"
				"\n"
				"    * Redistributions of source code must retain the above copyright\n"
				"notice, this list of conditions and the following disclaimer.\n"
				"\n"
				"    * Redistributions in binary form must reproduce the above\n"
				"copyright notice, this list of conditions and the following disclaimer\n"
				"in the documentation and/or other materials provided with the\n"
				"distribution.\n"
				"\n"
				"    * Neither the name of the Danga Interactive nor the names of its\n"
				"contributors may be used to endorse or promote products derived from\n"
				"this software without specific prior written permission.\n"
				"\n"
				"THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS\n"
				"\"AS IS\" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT\n"
				"LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR\n"
				"A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT\n"
				"OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,\n"
				"SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT\n"
				"LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,\n"
				"DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY\n"
				"THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT\n"
				"(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE\n"
				"OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.\n"
				"\n"
				"\n"
				"This product includes software developed by Niels Provos.\n"
				"\n"
				"[ libevent ]\n"
				"\n"
				"Copyright 2000-2003 Niels Provos <provos@citi.umich.edu>\n"
				"All rights reserved.\n"
				"\n"
				"Redistribution and use in source and binary forms, with or without\n"
				"modification, are permitted provided that the following conditions\n"
				"are met:\n"
				"1. Redistributions of source code must retain the above copyright\n"
				"   notice, this list of conditions and the following disclaimer.\n"
				"2. Redistributions in binary form must reproduce the above copyright\n"
				"   notice, this list of conditions and the following disclaimer in the\n"
				"   documentation and/or other materials provided with the distribution.\n"
				"3. All advertising materials mentioning features or use of this software\n"
				"   must display the following acknowledgement:\n"
				"      This product includes software developed by Niels Provos.\n"
				"4. The name of the author may not be used to endorse or promote products\n"
				"   derived from this software without specific prior written permission.\n"
				"\n"
				"THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR\n"
				"IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES\n"
				"OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.\n"
				"IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,\n"
				"INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT\n"
				"NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,\n"
				"DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY\n"
				"THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT\n"
				"(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF\n"
				"THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.\n");

return;
}

static void save_pid(const char *pid_file) {
FILE *fp;
if (access(pid_file, F_OK) == 0) {
	if ((fp = fopen(pid_file, "r")) != NULL ) {
		char buffer[1024];
		if (fgets(buffer, sizeof(buffer), fp) != NULL ) {
			unsigned int pid;
			if (safe_strtoul(buffer, &pid) && kill((pid_t) pid, 0) == 0) {
				fprintf(stderr,
						"WARNING: The pid file contained the following (running) pid: %u\n",
						pid);
			}
		}
		fclose(fp);
	}
}

if ((fp = fopen(pid_file, "w")) == NULL ) {
	vperror("Could not open the pid file %s for writing", pid_file);
	return;
}

fprintf(fp, "%ld\n", (long) getpid());
if (fclose(fp) == -1) {
	vperror("Could not close the pid file %s", pid_file);
}
}

static void remove_pidfile(const char *pid_file) {
if (pid_file == NULL )
	return;

if (unlink(pid_file) != 0) {
	vperror("Could not remove the pid file %s", pid_file);
}

}

static void sig_handler(const int sig) {
printf("SIGINT handled.\n");
exit(EXIT_SUCCESS);
}

#ifndef HAVE_SIGIGNORE
static int sigignore(int sig) {
struct sigaction sa = { .sa_handler = SIG_IGN, .sa_flags = 0 };

if (sigemptyset(&sa.sa_mask) == -1 || sigaction(sig, &sa, 0) == -1) {
	return -1;
}
return 0;
}
#endif

/*
 * On systems that supports multiple page sizes we may reduce the
 * number of TLB-misses by using the biggest available page size
 */
static int enable_large_pages(void) {
#if defined(HAVE_GETPAGESIZES) && defined(HAVE_MEMCNTL)
int ret = -1;
size_t sizes[32];
int avail = getpagesizes(sizes, 32);
if (avail != -1) {
	size_t max = sizes[0];
	struct memcntl_mha arg = {0};
	int ii;

	for (ii = 1; ii < avail; ++ii) {
		if (max < sizes[ii]) {
			max = sizes[ii];
		}
	}

	arg.mha_flags = 0;
	arg.mha_pagesize = max;
	arg.mha_cmd = MHA_MAPSIZE_BSSBRK;

	if (memcntl(0, 0, MC_HAT_ADVISE, (caddr_t)&arg, 0, 0) == -1) {
		fprintf(stderr, "Failed to set large pages: %s\n",
				strerror(errno));
		fprintf(stderr, "Will use default page size\n");
	} else {
		ret = 0;
	}
} else {
	fprintf(stderr, "Failed to get supported pagesizes: %s\n",
			strerror(errno));
	fprintf(stderr, "Will use default page size\n");
}

return ret;
#else
return -1;
#endif
}

/**
 * Do basic sanity check of the runtime environment
 * @return true if no errors found, false if we can't use this env
 */
static bool sanitycheck(void) {
/* One of our biggest problems is old and bogus libevents */
const char *ever = event_get_version();
if (ever != NULL ) {
	if (strncmp(ever, "1.", 2) == 0) {
		/* Require at least 1.3 (that's still a couple of years old) */
		if ((ever[2] == '1' || ever[2] == '2') && !isdigit(ever[3])) {
			fprintf(stderr, "You are using libevent %s.\nPlease upgrade to"
					" a more recent version (1.3 or newer)\n",
					event_get_version());
			return false;
		}
	}
}

return true;
}

static void connect_to_bootstrap(char *bootstrap_port_no){
	int MAXDATASIZE=1024;
	int sockfd=0, numbytes;
	char buf[MAXDATASIZE];
	int i;
	char buf2[255];
	int temp;

	strcpy(join_server_ip_address,"localhost");
	fprintf(stderr,"\nBootstrap is at %s:%s\n",join_server_ip_address,bootstrap_port_no);
    sockfd = connect_to("localhost", "11311","connect_to_boostrap");
    //receiving join req port
	if ((numbytes = recv(sockfd, buf, MAXDATASIZE-1, 0)) == -1) {
	perror("recv");
	exit(1);
	}

	buf[numbytes] = '\0';
	printf("client: received '%s'\n",buf);


	sprintf(me.join_request,"%s",buf);

	//receiving world boundaries
    world_boundary = *(_recv_boundary_from_neighbour(sockfd));
	me.boundary=world_boundary;


////receiving whom to connect
	memset(buf,'\0',1024);
		if ((numbytes = recv(sockfd, buf, MAXDATASIZE-1, 0)) == -1) {
			perror("recv");
			exit(1);
			}
		printf("client: received '%s'\n",buf);
		sscanf(buf,"%s %d",buf2,&temp);
		printf("client: received buf2:'%s'\n",buf2);
		if(!strcmp(buf2,"NOTFIRST"))
		{
			sprintf(join_server_port_number,"%d",temp);
			fprintf(stderr,"\nNode starting as Child, connecting to %s to receive keys\n",join_server_port_number);
			starting_node_type = START_AS_CHILD;
		}
		else
		{
            fprintf(stderr,"\nNode starting as Parent\n");
		    starting_node_type = START_AS_PARENT;
		}
	close(sockfd);
    for(i =0 ;i < 10 ;i++)
    {
        strcpy(neighbour[i].node_removal,"NULL");
        strcpy(neighbour[i].request_propogation,"NULL");
    }
}

static void my_init(){
    NULL_NODE_INFO.boundary = NULL_BOUNDARY;
    strcpy(NULL_NODE_INFO.join_request,"NULL");
    strcpy(NULL_NODE_INFO.request_propogation,"NULL");
    strcpy(NULL_NODE_INFO.node_removal,"NULL");
    int i=0;
    for(i = 0;i<10;i++){
        copy_node_info(NULL_NODE_INFO,&neighbour[i]);
    }
}

int main(int argc, char **argv) {
int c;
bool lock_memory = false;
bool do_daemonize = false;
bool preallocate = false;
int maxcore = 0;
char *username = NULL;
char *pid_file = NULL;
struct passwd *pw;
struct rlimit rlim;
char unit = '\0';
int size_max = 0;
int retval = EXIT_SUCCESS;
char temp[255];
/* listening sockets */
static int *l_socket = NULL;

/* udp socket */
static int *u_socket = NULL;
bool protocol_specified = false;
bool tcp_specified = false;
bool udp_specified = false;

char *subopts;
char *subopts_value;
enum {
	MAXCONNS_FAST = 0, HASHPOWER_INIT, SLAB_REASSIGN, SLAB_AUTOMOVE
};
char * const subopts_tokens[] = { [MAXCONNS_FAST] = "maxconns_fast",
		[HASHPOWER_INIT] = "hashpower", [SLAB_REASSIGN] = "slab_reassign",
		[SLAB_AUTOMOVE] = "slab_automove", NULL };

if (!sanitycheck()) {
	return EX_OSERR;
}

/* handle SIGINT */
signal(SIGINT, sig_handler);

/* init settings */
settings_init();

my_init();

/* set stderr non-buffering (for running under, say, daemontools) */
setbuf(stderr, NULL );

char *ptr;

/* process arguments */
while (-1 != (c = getopt(argc, argv, "a:" /* access mask for unix socket */
		"A" /* enable admin shutdown commannd */
		"p:" /* TCP port number to listen on */
		"s:" /* unix socket path to listen on */
		"U:" /* UDP port number to listen on */
		"m:" /* max memory to use for items in megabytes */
		"M" /* return error on memory exhausted */
		"c:" /* max simultaneous connections */
		"k" /* lock down all paged memory */
		"hi" /* help, licence info */
		"r" /* maximize core file limit */
		"v" /* verbose */
		"d" /* daemon mode */
		"l:" /* interface to listen on */
		"u:" /* user identity to run as */
		"P:" /* save PID in file */
		"f:" /* factor? */
		"n:" /* minimum space allocated for key+value+flags */
		"t:" /* threads */
		"D:" /* prefix delimiter? */
		"L" /* Large memory pages */
		"R:" /* max requests per event */
		"C" /* Disable use of CAS */
		"b:" /* backlog queue limit */
		"B:" /* Binding protocol */
		"I:" /* Max item size */
		"S" /* Sasl ON */
		"o:" /* Extended generic options */
		"x:" /* lower x coordinate */
		"y:" /* lower y coordinate */
		"X:" /* upper x coordinate */
		"Y:" /* upper y coordinate */
		"j:" /*IP address and port number of the node to join with */
		"J:" /*Connecting to bootstrap*/
))) {
	switch (c) {
	case 'A':
		/* enables "shutdown" command */
		settings.shutdown_command = true;
		break;


	case 'x':
		me.boundary.from.x = atof(optarg);
		world_boundary.from.x = me.boundary.from.x;
		break;
	case 'X':
		me.boundary.to.x = atof(optarg);
		world_boundary.to.x = me.boundary.to.x;
		break;
	case 'y':
		me.boundary.from.y = atof(optarg);
		world_boundary.from.y = me.boundary.from.y;
		break;
	case 'Y':
		me.boundary.to.y = atof(optarg);
		world_boundary.to.y = me.boundary.to.y;
		break;

	case 'j':
		starting_node_type = START_AS_PARENT;
		ptr = strtok(optarg, ":");
		strcpy(join_server_ip_address, ptr);
		ptr = strtok(NULL, ":");
		strcpy(join_server_port_number, ptr);

		break;
	case 'J':
		strcpy(temp,optarg);
		connect_to_bootstrap(temp);
		break;
	case 'a':
		/* access for unix domain socket, as octal mask (like chmod)*/
		settings.access = strtol(optarg, NULL, 8);
		break;

	case 'U':
		settings.udpport = atoi(optarg);
		udp_specified = true;
		break;
	case 'p':
		settings.port = atoi(optarg);
		tcp_specified = true;
		break;
	case 's':
		settings.socketpath = optarg;
		break;
	case 'm':
		settings.maxbytes = ((size_t) atoi(optarg)) * 1024 * 1024;
		break;
	case 'M':
		settings.evict_to_free = 0;
		break;
	case 'c':
		settings.maxconns = atoi(optarg);
		break;
	case 'h':
		usage();
		exit(EXIT_SUCCESS);
	case 'i':
		usage_license();
		exit(EXIT_SUCCESS);
	case 'k':
		lock_memory = true;
		break;
	case 'v':
		settings.verbose++;
		break;
	case 'l':
		if (settings.inter != NULL ) {
			size_t len = strlen(settings.inter) + strlen(optarg) + 2;
			char *p = malloc(len);
			if (p == NULL ) {
				fprintf(stderr, "Failed to allocate memory\n");
				return 1;
			}
			snprintf(p, len, "%s,%s", settings.inter, optarg);
			free(settings.inter);
			settings.inter = p;
		} else {
			settings.inter = strdup(optarg);
		}
		break;
	case 'd':
		do_daemonize = true;
		break;
	case 'r':
		maxcore = 1;
		break;
	case 'R':
		settings.reqs_per_event = atoi(optarg);
		if (settings.reqs_per_event == 0) {
			fprintf(stderr,
					"Number of requests per event must be greater than 0\n");
			return 1;
		}
		break;
	case 'u':
		username = optarg;
		break;
	case 'P':
		pid_file = optarg;
		break;
	case 'f':
		settings.factor = atof(optarg);
		if (settings.factor <= 1.0) {
			fprintf(stderr, "Factor must be greater than 1\n");
			return 1;
		}
		break;
	case 'n':
		settings.chunk_size = atoi(optarg);
		if (settings.chunk_size == 0) {
			fprintf(stderr, "Chunk size must be greater than 0\n");
			return 1;
		}
		break;
	case 't':
		settings.num_threads = atoi(optarg);
		if (settings.num_threads <= 0) {
			fprintf(stderr, "Number of threads must be greater than 0\n");
			return 1;
		}
		/* There're other problems when you get above 64 threads.
		 * In the future we should portably detect # of cores for the
		 * default.
		 */
		if (settings.num_threads > 64) {
			fprintf(stderr, "WARNING: Setting a high number of worker"
					"threads is not recommended.\n"
					" Set this value to the number of cores in"
					" your machine or less.\n");
		}
		break;
	case 'D':
		if (!optarg || !optarg[0]) {
			fprintf(stderr, "No delimiter specified\n");
			return 1;
		}
		settings.prefix_delimiter = optarg[0];
		settings.detail_enabled = 1;
		break;
	case 'L':
		if (enable_large_pages() == 0) {
			preallocate = true;
		} else {
			fprintf(stderr, "Cannot enable large pages on this system\n"
					"(There is no Linux support as of this version)\n");
			return 1;
		}
		break;
	case 'C':
		settings.use_cas = false;
		break;
	case 'b':
		settings.backlog = atoi(optarg);
		break;
	case 'B':
		protocol_specified = true;
		if (strcmp(optarg, "auto") == 0) {
			settings.binding_protocol = negotiating_prot;
		} else if (strcmp(optarg, "binary") == 0) {
			settings.binding_protocol = binary_prot;
		} else if (strcmp(optarg, "ascii") == 0) {
			settings.binding_protocol = ascii_prot;
		} else {
			fprintf(stderr, "Invalid value for binding protocol: %s\n"
					" -- should be one of auto, binary, or ascii\n", optarg);
			exit(EX_USAGE);
		}
		break;
	case 'I':
		unit = optarg[strlen(optarg) - 1];
		if (unit == 'k' || unit == 'm' || unit == 'K' || unit == 'M') {
			optarg[strlen(optarg) - 1] = '\0';
			size_max = atoi(optarg);
			if (unit == 'k' || unit == 'K')
				size_max *= 1024;
			if (unit == 'm' || unit == 'M')
				size_max *= 1024 * 1024;
			settings.item_size_max = size_max;
		} else {
			settings.item_size_max = atoi(optarg);
		}
		if (settings.item_size_max < 1024) {
			fprintf(stderr, "Item max size cannot be less than 1024 bytes.\n");
			return 1;
		}
		if (settings.item_size_max > 1024 * 1024 * 128) {
			fprintf(stderr, "Cannot set item size limit higher than 128 mb.\n");
			return 1;
		}
		if (settings.item_size_max > 1024 * 1024) {
			fprintf(stderr,
					"WARNING: Setting item max size above 1MB is not"
							" recommended!\n"
							" Raising this limit increases the minimum memory requirements\n"
							" and will decrease your memory efficiency.\n");
		}
		break;
	case 'S': /* set Sasl authentication to true. Default is false */
#ifndef ENABLE_SASL
		fprintf(stderr, "This server is not built with SASL support.\n");
		exit(EX_USAGE);
#endif
		settings.sasl = true;
		break;
	case 'o': /* It's sub-opts time! */
		subopts = optarg;

		while (*subopts != '\0') {

			switch (getsubopt(&subopts, subopts_tokens, &subopts_value)) {
			case MAXCONNS_FAST:
				settings.maxconns_fast = true;
				break;
			case HASHPOWER_INIT:
				if (subopts_value == NULL ) {
					fprintf(stderr, "Missing numeric argument for hashpower\n");
					return 1;
				}
				settings.hashpower_init = atoi(subopts_value);
				if (settings.hashpower_init < 12) {
					fprintf(stderr,
							"Initial hashtable multiplier of %d is too low\n",
							settings.hashpower_init);
					return 1;
				} else if (settings.hashpower_init > 64) {
					fprintf(stderr,
							"Initial hashtable multiplier of %d is too high\n"
									"Choose a value based on \"STAT hash_power_level\" from a running instance\n",
							settings.hashpower_init);
					return 1;
				}
				break;
			case SLAB_REASSIGN:
				settings.slab_reassign = true;
				break;
			case SLAB_AUTOMOVE:
				if (subopts_value == NULL ) {
					settings.slab_automove = 1;
					break;
				}
				settings.slab_automove = atoi(subopts_value);
				if (settings.slab_automove < 0 || settings.slab_automove > 2) {
					fprintf(stderr, "slab_automove must be between 0 and 2\n");
					return 1;
				}
				break;
			default:
				printf("Illegal suboption \"%s\"\n", subopts_value);
				return 1;
			}

		}
		break;
	default:
		fprintf(stderr, "Illegal argument \"%c\"\n", c);
		return 1;
	}
}

/*
 * Use one workerthread to serve each UDP port if the user specified
 * multiple ports
 */
if (settings.inter != NULL && strchr(settings.inter, ',')) {
	settings.num_threads_per_udp = 1;
} else {
	settings.num_threads_per_udp = settings.num_threads;
}

if (settings.sasl) {
	if (!protocol_specified) {
		settings.binding_protocol = binary_prot;
	} else {
		if (settings.binding_protocol != binary_prot) {
			fprintf(stderr,
					"ERROR: You cannot allow the ASCII protocol while using SASL.\n");
			exit(EX_USAGE);
		}
	}
}

if (tcp_specified && !udp_specified) {
	settings.udpport = settings.port;
} else if (udp_specified && !tcp_specified) {
	settings.port = settings.udpport;
}

if (maxcore != 0) {
	struct rlimit rlim_new;
	/*
	 * First try raising to infinity; if that fails, try bringing
	 * the soft limit to the hard.
	 */
	if (getrlimit(RLIMIT_CORE, &rlim) == 0) {
		rlim_new.rlim_cur = rlim_new.rlim_max = RLIM_INFINITY;
		if (setrlimit(RLIMIT_CORE, &rlim_new) != 0) {
			/* failed. try raising just to the old max */
			rlim_new.rlim_cur = rlim_new.rlim_max = rlim.rlim_max;
			(void) setrlimit(RLIMIT_CORE, &rlim_new);
		}
	}
	/*
	 * getrlimit again to see what we ended up with. Only fail if
	 * the soft limit ends up 0, because then no core files will be
	 * created at all.
	 */

	if ((getrlimit(RLIMIT_CORE, &rlim) != 0) || rlim.rlim_cur == 0) {
		fprintf(stderr, "failed to ensure corefile creation\n");
		exit(EX_OSERR);
	}
}

/*
 * If needed, increase rlimits to allow as many connections
 * as needed.
 */

if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
	fprintf(stderr, "failed to getrlimit number of files\n");
	exit(EX_OSERR);
} else {
	rlim.rlim_cur = settings.maxconns;
	rlim.rlim_max = settings.maxconns;
	if (setrlimit(RLIMIT_NOFILE, &rlim) != 0) {
		fprintf(stderr,
				"failed to set rlimit for open files. Try starting as root or requesting smaller maxconns value.\n");
		exit(EX_OSERR);
	}
}

/* lose root privileges if we have them */
if (getuid() == 0 || geteuid() == 0) {
	if (username == 0 || *username == '\0') {
		fprintf(stderr, "can't run as root without the -u switch\n");
		exit(EX_USAGE);
	}
	if ((pw = getpwnam(username)) == 0) {
		fprintf(stderr, "can't find the user %s to switch to\n", username);
		exit(EX_NOUSER);
	}
	if (setgid(pw->pw_gid) < 0 || setuid(pw->pw_uid) < 0) {
		fprintf(stderr, "failed to assume identity of user %s\n", username);
		exit(EX_OSERR);
	}
}

/* Initialize Sasl if -S was specified */
if (settings.sasl) {
	init_sasl();
}

/* daemonize if requested */
/* if we want to ensure our ability to dump core, don't chdir to / */
if (do_daemonize) {
	if (sigignore(SIGHUP) == -1) {
		perror("Failed to ignore SIGHUP");
	}
	if (daemonize(maxcore, settings.verbose) == -1) {
		fprintf(stderr, "failed to daemon() in order to daemonize\n");
		exit(EXIT_FAILURE);
	}
}

/* lock paged memory if needed */
if (lock_memory) {
#ifdef HAVE_MLOCKALL
	int res = mlockall(MCL_CURRENT | MCL_FUTURE);
	if (res != 0) {
		fprintf(stderr, "warning: -k invalid, mlockall() failed: %s\n",
				strerror(errno));
	}
#else
	fprintf(stderr,
			"warning: -k invalid, mlockall() not supported on this platform.  proceeding without.\n");
#endif
}

/* initialize main thread libevent instance */
main_base = event_init();

/* initialize other stuff */
stats_init();
assoc_init(settings.hashpower_init);
conn_init();
slabs_init(settings.maxbytes, settings.factor, preallocate);

/*
 * ignore SIGPIPE signals; we can use errno == EPIPE if we
 * need that information
 */
if (sigignore(SIGPIPE) == -1) {
	perror("failed to ignore SIGPIPE; sigaction");
	exit(EX_OSERR);
}

pthread_mutex_init(&list_of_keys_lock, NULL );

pthread_mutex_lock(&list_of_keys_lock);
mylist_init("all_keys",&list_of_keys);
mylist_init("trash_both",&trash_both);
pthread_mutex_unlock(&list_of_keys_lock);

/* start up worker threads if MT mode */
if (starting_node_type == START_AS_PARENT) {
	mode = NORMAL_NODE;

	fprintf(stderr, "Mode set as : NORMAL_NODE\n");
	print_ecosystem();
	thread_init(settings.num_threads, main_base,
			join_request_listener_thread_routine, NULL,
			node_removal_listener_thread_routine,
			node_propagation_thread_routine);
} else if (starting_node_type == START_AS_CHILD) {
    mode = SPLITTING_CHILD_INIT;
    fprintf(stderr, "Mode set as : SPLITTING_CHILD_INIT\n");
	thread_init(settings.num_threads, main_base, NULL,
			connect_and_split_thread_routine,
			node_removal_listener_thread_routine,
			node_propagation_thread_routine);
}
else {
    fprintf(stderr,"Invalid start node type\n");
    exit(-1);
}

if (start_assoc_maintenance_thread() == -1) {
	exit(EXIT_FAILURE);
}

if (settings.slab_reassign && start_slab_maintenance_thread() == -1) {
	exit(EXIT_FAILURE);
}

/* initialise clock event */
clock_handler(0, 0, 0);

/* create unix mode sockets after dropping privileges */
if (settings.socketpath != NULL ) {
	errno = 0;
	if (server_socket_unix(settings.socketpath, settings.access)) {
		vperror("failed to listen on UNIX socket: %s", settings.socketpath);
		exit(EX_OSERR);
	}
}

/* create the listening socket, bind it, and init */
if (settings.socketpath == NULL ) {
	const char *portnumber_filename = getenv("MEMCACHED_PORT_FILENAME");
	char temp_portnumber_filename[PATH_MAX];
	FILE *portnumber_file = NULL;

	if (portnumber_filename != NULL ) {
		snprintf(temp_portnumber_filename, sizeof(temp_portnumber_filename),
				"%s.lck", portnumber_filename);

		portnumber_file = fopen(temp_portnumber_filename, "a");
		if (portnumber_file == NULL ) {
			fprintf(stderr, "Failed to open \"%s\": %s\n",
					temp_portnumber_filename, strerror(errno));
		}
	}

	errno = 0;
	if (settings.port
			&& server_sockets(settings.port, tcp_transport, portnumber_file)) {
		vperror("failed to listen on TCP port %d", settings.port);
		exit(EX_OSERR);
	}

	/*
	 * initialization order: first create the listening sockets
	 * (may need root on low ports), then drop root if needed,
	 * then daemonise if needed, then init libevent (in some cases
	 * descriptors created by libevent wouldn't survive forking).
	 */

	/* create the UDP listening socket and bind it */errno = 0;
	if (settings.udpport
			&& server_sockets(settings.udpport, udp_transport,
					portnumber_file)) {
		vperror("failed to listen on UDP port %d", settings.udpport);
		exit(EX_OSERR);
	}

	if (portnumber_file) {
		fclose(portnumber_file);
		rename(temp_portnumber_filename, portnumber_filename);
	}
}

/* Give the sockets a moment to open. I know this is dumb, but the error
 * is only an advisory.
 */
usleep(1000);
if (stats.curr_conns + stats.reserved_fds >= settings.maxconns - 1) {
	fprintf(stderr, "Maxconns setting is too low, use -c to increase.\n");
	exit(EXIT_FAILURE);
}

if (pid_file != NULL ) {
	save_pid(pid_file);
}

/* Drop privileges no longer needed */
drop_privileges();

print_all_boundaries();
/* enter the event loop */
if (event_base_loop(main_base, 0) != 0) {
	retval = EXIT_FAILURE;
}

stop_assoc_maintenance_thread();

/* remove the PID file if we're a daemon */
if (do_daemonize)
	remove_pidfile(pid_file);
/* Clean up strdup() call for bind() address */
if (settings.inter)
	free(settings.inter);
if (l_socket)
	free(l_socket);
if (u_socket)
	free(u_socket);

return retval;
}
