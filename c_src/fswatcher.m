// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Console app that prints path to folder where a file has changed.
#include <objc/Object.h>
#include <stdio.h>
#include <stdlib.h>
#include <CoreFoundation/CFArray.h>
#include <Foundation/NSArray.h>
#include <Foundation/NSString.h>
#include <CoreFoundation/CFRunLoop.h>
#include <CoreFoundation/CFUUID.h>
#include <CoreServices/CoreServices.h>
#include <CoreFoundation/CFDate.h>

// gcc -lobjc -framework CoreFoundation -framework CoreServices c_src/fswatcher.m -o /priv/fswatcher

void EventStreamCallback(ConstFSEventStreamRef sref, void* user_data, size_t numEvents, void* eventPaths,
                                  const FSEventStreamEventFlags sflags[], const FSEventStreamEventId seid[]);

void EventStreamCallback(ConstFSEventStreamRef sref, void* user_data, size_t numEvents, void* eventPaths,
                                  const FSEventStreamEventFlags sflags[], const FSEventStreamEventId seid[])
{
    char** paths = (char**)(eventPaths);

    int i = 0;
    for (i = 0; i < numEvents; ++i)
    {
        printf("%s\r\n",paths[i]);
    }
    fflush(stdout);
}
static void user_input(CFSocketRef s, CFSocketCallBackType type, 
	   CFDataRef address, const void *data, void *info)
{
	char str[100];
	char* ret = fgets(str, sizeof(str), stdin);
	if (ret == NULL || !CFSocketIsValid(s))
	{
		CFRunLoopStop(CFRunLoopGetCurrent());
	}
}





int main(int argc, char *argv[])
{
  int i = 0;
	if (argc <= 1)
	{
		printf("Usage: %s root/path/to/watch\r\n",argv[0]);
		return EXIT_SUCCESS;
	}

  for (i = 1; i < argc; i++)
  {
    CFAbsoluteTime latency = 0.5;
    CFStringRef pathstr  = CFStringCreateWithCString(kCFAllocatorDefault, argv[i], kCFStringEncodingUTF8);
    CFTypeRef   cfValues[] = { pathstr };
    CFArrayRef paths = CFArrayCreate( kCFAllocatorDefault, cfValues, 1, &kCFTypeArrayCallBacks );

    FSEventStreamRef fsstream = FSEventStreamCreate(NULL,
           &EventStreamCallback,
           NULL,
           paths,
           kFSEventStreamEventIdSinceNow,
           latency,
           kFSEventStreamCreateFlagNone
     );

    FSEventStreamScheduleWithRunLoop(fsstream, CFRunLoopGetCurrent(), kCFRunLoopDefaultMode);
    FSEventStreamStart(fsstream);
  }

  CFSocketRef	socket;
  socket = CFSocketCreateWithNative(NULL, fileno(stdin), kCFSocketReadCallBack, user_input, NULL);
  CFRunLoopSourceRef rls = CFSocketCreateRunLoopSource(NULL, socket, 0);
  CFRunLoopAddSource(CFRunLoopGetCurrent(), rls, kCFRunLoopDefaultMode);

  CFRunLoopRun();

	return EXIT_SUCCESS;
}




