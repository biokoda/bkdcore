// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// This is not really used...

#include <stdio.h>
#include <limits.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <linux/inotify.h>

#define EVENT_SIZE  ( sizeof (struct inotify_event) )
#define EVENT_BUF_LEN     (128 * (EVENT_SIZE+16+NAME_MAX))

int main(int argc, char *argv[])
{
  if (argc < 2)
  {
    printf("Usage: %s root/path/to/watch /anotherpath/to/watch  .. \r\n",argv[0]);
    return EXIT_SUCCESS;
  }
  int length, i = 0;
  int notifyfd;
  int wd;
  char buffer[EVENT_BUF_LEN];
  int watches[argc-1];
  fd_set selectset;
  FD_ZERO(&selectset);
  FD_SET(0, &selectset);

  /*creating the INOTIFY instance*/
  notifyfd = inotify_init();
  FD_SET(notifyfd, &selectset);

  /*checking for error*/
  if ( notifyfd < 0 ) 
  {
    perror("inotify_init failed");
  }

  for (i = 1; i < argc; i++)
  {
    watches[i-1] = inotify_add_watch(notifyfd, argv[i], IN_CREATE | IN_MODIFY);
  }
    

  
  for (;;)
  {
    fd_set readset = selectset;
    if (select(notifyfd+1, &readset, NULL, NULL, NULL) == -1)
    {
      perror("Unable to select");
      return 0;
    }
    
    if (FD_ISSET(0,&readset))
      break;
    else
    {
      length = read(notifyfd, buffer, EVENT_BUF_LEN); 
      if (length < 0) 
      {
        perror("unable to read from inotify");
        return 0;
      }  

      i = 0;
      while(i < length) 
      {     
        struct inotify_event *event = (struct inotify_event*)&buffer[i];     
        if (event->len)
        {
          int j;
          for (j = 1; j < argc; j++)
          {
            if (watches[j-1] == event->wd)
            {
              printf("%s/%s\r\n", argv[j], event->name);
              break;
            }
          }
        }
        i += EVENT_SIZE + event->len;
      }
    }
  }

  for (i = 1; i < argc; i++)
    inotify_rm_watch(notifyfd, watches[i-1]);
  close(notifyfd);
}