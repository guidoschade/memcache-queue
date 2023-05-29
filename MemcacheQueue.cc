#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <syslog.h>
#include "MemcacheQueue.h"

// memcachequeue constructor
MemcacheQueue::MemcacheQueue(const char * qn = "unknown", const char * h1 = "localhost", int p1 = 11211, int st = 30000, int to = 1, int ct = 1, int nd = 10)
{
  // copy data to class to avoid using pointers
  strncpy(qname, qn, sizeof(qname));
  snprintf(qaccess, sizeof(qaccess), "%s_access", qn);
  snprintf(qhead, sizeof(qhead), "%s_head", qn);
  snprintf(qtail, sizeof(qtail), "%s_tail", qn);
  strncpy(host1, h1, sizeof(host1));
  port1 = p1;
  sleepTime = st;
  timeout = to;
  connect_tries = ct;
  snprintf(keylock, sizeof(keylock), "%s_%d", "test", nd);

  // setting default values
  access = 0;
  connected = 0;
  debug = 3;
  ver = 1.3;
  servers = NULL;

  // connect to the memcache server
  connect();
}

// logging
void MemcacheQueue::mlog(int level, const char * entry)
{
  char str[256];
  if (debug >= level)
  {
    snprintf(str, 250, "memq_%2.1f (%s:%d/%s) %s", ver, host1, port1, qname, entry);
    syslog(LOG_WARNING, str);
    //if (level > 5) { printf("%s\n", entry); }
    { printf("%s\n", entry); }
  }
}

// set node
int MemcacheQueue::setNode(int nname) { snprintf(keylock, sizeof(keylock), "%d", nname); return(0); }

// get connected state
int MemcacheQueue::isConnected() { return(connected); }

// get length / size of given queue (queue size)
int MemcacheQueue::getSize()
{
  // debugging
  mlog(6,"getsize called");

  char * rhead, * rtail;

  // make sure we are connected to memcached and got lock
  if (!connected) { connect(); }
  if (!access) { getLock(); }

  if (connected && access)
  {
    // get head and tail from memcache
    rhead = memcached_get(memc, qhead, strlen(qhead), &value_length, &flags, &rc);
    if (rhead != NULL) { head = atoi((char *)rhead); free(rhead); } else { head = 0; }
    if (rc != MEMCACHED_SUCCESS) { mlog(1,"getsize get head failed"); return(0); }

    rtail = memcached_get(memc, qtail, strlen(qtail), &value_length, &flags, &rc);
    if (rtail != NULL) { tail = atoi((char *)rtail); free(rtail); } else { tail = 0; }
    if (rc != MEMCACHED_SUCCESS) { mlog(1,"getsize get tail failed"); return(0); }

    // return size of queue - 0 = empty
    return(tail - head);
  }
  else
  {
    mlog(1,"getsize - error during lock or access");
    return(0);
  }
}

// set head
int MemcacheQueue::setHead(int position = 1)
{
  mlog(6,"setHead called");

  // create new head string and set head - without expiry
  snprintf(value, sizeof(value), "%d", position);
  rc = memcached_set(memc, qhead, strlen(qhead), value, strlen(value), (time_t)0, (uint32_t)0);
  if (rc != MEMCACHED_SUCCESS) { mlog(1,"error: failed to set head"); return(1); }
  head = position;
  return(0);
}

// set tail
int MemcacheQueue::setTail(int position = 1)
{
  mlog(6,"setTail called");

  // create new tail string and set tail - without expiry
  snprintf(value, sizeof(value), "%d", position);
  rc = memcached_set(memc, qtail, strlen(qtail), value, strlen(value), (time_t)0, (uint32_t)0);
  if (rc != MEMCACHED_SUCCESS) { mlog(1,"error: failed to set tail"); return(1); }
  tail = position;
  return(0);
}

// connect to server - try until successful
int MemcacheQueue::connect(void)
{
  mlog(6,"connect called");
  int tries = connect_tries;
  char * rhead = NULL;
  char * rtail = NULL;

  // only if not connected already
  if (!connected)
  {
    while(1)
    {
      // create new memcache object
      memc = memcached_create(NULL);
      servers = memcached_server_list_append(servers, host1, port1, &rc);
      rc = memcached_server_push(memc, servers);

      if (rc == MEMCACHED_SUCCESS)
      {
        connected = 1;
        mlog(6,"connect success");

        // if head or tail does not exist, create it
        rhead = memcached_get(memc, qhead, strlen(qhead), &value_length, &flags, &rc);
        if (rhead != NULL) { head = atoi((char *)rhead); free(rhead); } else { head = 0; }
        if (rc != MEMCACHED_SUCCESS) { setHead(); }
        rtail = memcached_get(memc, qtail, strlen(qtail), &value_length, &flags, &rc);
        if (rtail != NULL) { tail = atoi((char *)rtail); free(rtail); } else { tail = 0; }
        if (rc != MEMCACHED_SUCCESS) { setTail(); }

        return(1);
      }
      else // not connected
      {
        // connect to server has failed - unset stuff, sleep and try again
        mlog(1,"connect server1 error");
        memcached_free(memc);

        // exit if tries have been reached
        if (tries-- <= 0) { mlog(1,"connect server1 giving up"); break; }
      
        // sleep longer if the timeout higher
        if (timeout > 1) { sleep(timeout); } else { usleep(250000); }
      }
    } // while
  }
 
  // failed to connect
  return(0);
}

// get lock
int MemcacheQueue::getLock(void)
{
  mlog(6,"getLock called");

  int locktries = 50;
  char * lockedby = NULL;

  // make sure we are connected first
  if (!connected) { connect(); }

  // only if not connected yet - and has no lock
  if (connected && !access)
  {
    if (memc == NULL) { mlog(1, "error, not connected"); return(0); }

    // connected, wait to get lock - 10 seconds expiry on the lock - in case the process crashes
    while ((memcached_add(memc, qaccess, strlen(qaccess), keylock, strlen(keylock), (time_t) 10, (uint32_t)0) != MEMCACHED_SUCCESS) && locktries > 0)
    {
      // try to get owner of lock
      if (!lockedby) { lockedby = memcached_get(memc, qaccess, strlen(qaccess), &value_length, &flags, &rc); }

      // sleeping sleeptime ms (30-60 by default) and try to lock again - 2.5 seconds max
      usleep(sleepTime);
      locktries --;
    }
  
    // only acquire lock if not reached maximum tries
    if (locktries > 0) { access = 1; return(1); }
    else
    {
      access = 0;
      mlog(1,"getlock - locking failed or server unavailable");
      return(0);
    }
  } // if not access

  return(0);
}

// release lock
int MemcacheQueue::releaseLock()
{
  mlog(6,"releaseLock called");
  rc = memcached_delete (memc, qaccess, strlen(qaccess), (time_t)0);
  if (rc != MEMCACHED_SUCCESS) { mlog(1,"releaseLock failed"); return(0); }
  return(1);
}

// close - release lock and close connection
int MemcacheQueue::close()
{
  // debugging
  mlog(6,"close called");

  if (connected)
  {
    // release lock and close memcached connection
    if (access) { releaseLock(); }
    memcached_free(memc);
    connected = 0;
    return(1);
  } else { return(0); }
}

// add message to queue
char * MemcacheQueue::addMessage(char * data)
{
  // debugging
  mlog(6,"addmessage called");

  char * rhead, * rtail;
  int position = 0;

  // make sure we are connected to memcached and got lock
  if (!connected) { connect(); }
  if (!access) { getLock(); }

  if (connected && access)
  {
    // get head and tail from memcache
    rhead = memcached_get(memc, qhead, strlen(qhead), &value_length, &flags, &rc);
    if (rhead != NULL) { head = atoi((char *)rhead); free(rhead); } else { head = 0; }
    if (rc != MEMCACHED_SUCCESS) { mlog(1, "addmessage get head failed"); return(NULL); }

    rtail = memcached_get(memc, qtail, strlen(qtail), &value_length, &flags, &rc);
    if (rtail != NULL) { tail = atoi((char *)rtail); free(rtail); } else { tail = 0; }
    if (rc != MEMCACHED_SUCCESS) { mlog(1, "addmessage get tail failed"); return(NULL); }

    // if the head or tail key does not exist exist, initialize first
    if (!head || !tail) { setHead(); setTail(); head = 1; tail = 1; }

    // set position and increment later
    position = tail;

    // build key and add data - should never expire
    snprintf(key, sizeof(key), "%s_%d", qname, position);
    rc = memcached_add(memc, key, strlen(key), data, strlen(data), (time_t)0, (uint32_t)0);
  			
    // check it worked
    if (rc != MEMCACHED_SUCCESS) { mlog(1,"addmessage - error during add"); return(NULL); }

    // increment tail position
    setTail(++position);
		
    return(key);
  }
  else
  {
    mlog(1,"addmessage - error during lock or access");
    return(NULL);
  }
}

// return current position key
char * MemcacheQueue::getCurrentData()
{
  // allocate new buffer for it - will need to free after
  char * c = (char*) malloc(50);
  if (c == NULL) { mlog(1,"getCurrentData - malloc failed"); exit(1); }
  snprintf(c,48,"%s_%d", qname, head);
  return(c);
}

// get one message from queue
char * MemcacheQueue::getMessage()
{
  // debugging
  mlog(6,"getmessage called");

  char * rhead, * rtail;
  int position = 0;

  // make sure we are connected to memcached and got lock
  if (!connected) { connect(); }
  if (!access) { getLock(); }

  if (connected && access)
  {
    // get head and tail from memcache
    rhead = memcached_get(memc, qhead, strlen(qhead), &value_length, &flags, &rc);
    if (rhead != NULL) { head = atoi((char *)rhead); free(rhead); } else { head = 0; }
    if (rc != MEMCACHED_SUCCESS) { mlog(1, "getmessage get head failed"); }

    rtail = memcached_get(memc, qtail, strlen(qtail), &value_length, &flags, &rc);
    if (rtail != NULL) { tail = atoi((char *)rtail); free(rtail); } else { tail = 0; }
    if (rc != MEMCACHED_SUCCESS) { mlog(1, "getmessage get tail failed"); }

    // if the key does not exist, return null
    if (!head || !tail)
    {
      mlog(1,"getmessage - no head or tail");
      return(NULL);
    }

    // no message possible, get out
    if (head == 1 && tail == 1) { mlog(6, "getmessage head and tail is one, no message"); return(NULL); }
  				
    // get message from memcached
    char * k = getCurrentData();
    char * v = memcached_get(memc, k, strlen(k), &value_length, &flags, &rc);

    if (rc != MEMCACHED_SUCCESS) { mlog(1, "getmessage get message failed"); mlog(1,k); mlog(1,v); }
  			
    // increment head position
    if (v != NULL)
    {
      // delete the message from the queue
      rc = memcached_delete (memc, k, strlen(k), (time_t)0);
      if (rc != MEMCACHED_SUCCESS) { mlog(1, "getmessage delete message failed"); }

      // increment the head position
      position = head;
      setHead(++position);
    }
  							
    // check if we could reset the queue to 1
    if ((head == tail) && (head > 1)) { setHead(); setTail(); }

    // free key name buffer - allocated in getCurrentData()
    free(k); k = NULL;
  	
    // return message - allocated by memcached_get
    return (v);
  }
  else
  {
    mlog(1,"getmessage - error during lock or access");
    return(NULL);
  }
}

// destructor - release the lock, get key lock, get head and tail
MemcacheQueue::~MemcacheQueue()
{
  if (debug) { mlog(6,"destruct called"); }
  if (connected) { if (access) { releaseLock(); }; close(); }
}
