#include <libmemcached/memcached.h>

// memcache queue class definition
class MemcacheQueue
{ 
  public:
    MemcacheQueue(const char *, const char *, int, int, int, int, int);
    ~MemcacheQueue();
    int connect();
    int setNode(int);
    int isConnected();
    char * addMessage(char *);
    char * getMessage();
    int getSize();
    int close();

  private:
    int releaseLock();
    int getLock();
    void mlog(int, const char *);
    char * getCurrentData();
    int setHead(int);
    int setTail(int);

    char qname[50];
    char qaccess[60];
    char qhead[60];
    char qtail[60];
    int sleepTime;
    int access;
    int head;
    int tail;
    char host1[50];
    int port1;
    int connected;
    int timeout;
    int debug;
    int connect_tries;
    float ver;
    char keylock[20];

    // define variables for memcached library
    memcached_server_st * servers;
    memcached_st * memc;
    memcached_return rc;
    char key[64];
    char value[64];
    size_t value_length;
    uint32_t flags; 
};
