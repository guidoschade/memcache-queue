#include <cstdlib>
#include <iostream>
#include <libmemcached/memcached.h>

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>

// internal includes
#include "MemcacheQueue.h"

int main(int argc, char **argv)
{
  char * ret;
  int size;

  printf("test started\n");

  // new queue, add and read messages
  for (int x = 0; x<10; x++)
  {
    MemcacheQueue myq((char *) "testa", (char*) "localhost", 11211, 30000, 1, 1, 100);
    if (myq.isConnected())
    {
      size = myq.getSize();
      printf("size = %d\n", size);
  
      ret = myq.addMessage("hello1");
      printf("key = %s\n", ret);
  
      ret = myq.addMessage("hello2");
      printf("key = %s\n", ret);
    
      size = myq.getSize();
      printf("size = %d\n", size);
  
      ret = myq.getMessage();
      printf("val = %s\n", ret);
      free(ret);
  
      ret = myq.getMessage();
      printf("val = %s\n", ret);
      free(ret);
  
      ret = myq.getMessage();
      printf("val = %s\n", ret);
      free(ret);
  
      ret = myq.getMessage();
      printf("val = %s\n", ret);
      free(ret);
  
      size = myq.getSize();
      printf("size = %d\n", size);

      // add a few messages
      for (int y = 0; y<10; y++) { myq.addMessage("testing"); }

      size = myq.getSize();
      printf("size = %d\n", size);

      // read a few messages
      for (int y = 0; y<10; y++) { myq.getMessage(); }
  
      size = myq.getSize();
      printf("size = %d\n", size);

      myq.close();
    }
  }

  return(0);
}
