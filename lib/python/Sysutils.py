#!/usr/bin/env python

# Global libraries
import os
import sys
import signal



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



def switch_user(rsv, user, desired_uid, desired_gid):
    """ If the current process is not set as the desired UID, set it now.  If we are not
    root then bail out """

    this_process_uid = os.getuid()
    if this_process_uid == desired_uid:
        rsv.log("INFO", "Invoked as the RSV user (%s)" % user, 4)
    else:
        if this_process_uid == 0:
            rsv.log("INFO", "Invoked as root.  Switching to '%s' user (uid: %s - gid: %s)" %
                    (user, desired_uid, desired_gid), 4)

            try:
                os.setgid(desired_gid)
                os.setuid(desired_uid)
                os.environ["USER"]     = user
                os.environ["USERNAME"] = user
                os.environ["LOGNAME"]  = user
            except OSError:
                rsv.log("ERROR", "Unable to switch to '%s' user (uid: %s - gid: %s)" %
                        (user, desired_uid, desired_gid), 4)
            
        else:
            # Todo - allow any user to run, but don't produce consumer records
            rsv.log("ERROR", "You can only run metrics as root or the RSV user (%s)." % user, 0)
            sys.exit(1)
