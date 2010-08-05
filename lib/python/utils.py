#!/usr/bin/env python

import os
import rsv
import signal
from time import strftime, gmtime

class TimeoutError(Exception):
    """ This defines an Exception that we can use if our system call times out """
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
            

def alarm_handler(signum, frame):
    raise TimeoutError("Systam call timed out")


def system_with_timeout(command, timeout):
    """ Run a system command with a timeout specified (in seconds).
    Returns:
      1) exit code
      2) STDOUT/STDERR (combined)

    I think this could be better done using the socket module, but we need
    Python 2.7 for that.
    """
    signal.signal(signal.SIGALRM, alarm_handler)

    if command.find("2>") == -1:
        command += " 2>&1"

    # todo - is this the right way to time out a system call?
    signal.alarm(timeout)
    try:
        child = os.popen(command)
        data = child.read()
        err = child.close()
    except TimeoutError:
        child.close()
        return (None, None)
        
    signal.alarm(0)
    return (err, data)



def system(command):
    """ Run a system command
    Returns:
      1) exit code
      2) STDOUT/STDERR (combined)

    I think this could be better done using the socket module, but we need
    Python 2.7 for that.
    """
    if command.find("2>") == -1:
        command += " 2>&1"

    child = os.popen(command)
    data = child.read()
    err = child.close()
    return (err, data)


def timestamp(local=False):
    """ When generating timestamps, we want to use UTC when communicating with
    the remote collector.  For example:
      2010-07-25T05:18:14Z

    However, it's nice to print a more readable time for the local display, for
    example:
      2010-07-25 00:18:14 CDT

    This is consistent with RSVv3
    """
    
    if local:
        return strftime("%Y-%m-%d %H:%M:%S %Z")
    else:
        return strftime("%Y-%m-%dT%H:%M:%SZ", gmtime())


def switch_user(user, desired_uid, desired_gid):
    """ If the current process is not set as the desired UID, set it now.  If we are not
    root then bail out """

    this_process_uid = os.getuid()
    if this_process_uid == desired_uid:
        rsv.log("Invoked as the RSV user (%s)" % user, 2, 4)
    else:
        if this_process_uid == 0:
            rsv.log("Invoked as root.  Switching to '%s' user (uid: %s - gid: %s)" %
                    (user, desired_uid, desired_gid), 2, 4)
            # todo - catch permissions exception here?
            os.setgid(desired_gid)
            os.setuid(desired_uid)
            os.environ["USER"]     = user
            os.environ["USERNAME"] = user
            os.environ["LOGNAME"]  = user
            
        else:
            rsv.log("You can only run metrics as root or the RSV user (%s)." % user, 1, 0)
            sys.exit(1)

