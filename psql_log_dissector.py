#!/usr/bin/env python
# Postgres log digestor
# Put the following in the postgres config:
#     log_line_prefix = '%c %t'
#     log_connections = on
#     log_disconnections = on
# looks for   52c5b6c6.2216  ___________  and reports when one of these opens and closes:
#
import re
import sys


connections = {}
uid_regexp = re.compile('^[^ ]+\.([^ ]+)')


def parse_line(line):
  matches = uid_regexp.search(line)
  if matches:
    closed = None
    if line.find('disconnection: session time:') != -1:
      closed = True
    return {
      'uid': matches.groups()[0],
      'line': line,
      'closed': closed,
    }
  else:
    return False


def calc_procpid(uid):
  n = uid.find('.') + 1
  session_id = uid[n:]
  return int(session_id, 16)


def add_line(uid, line, closed=None):
  if uid not in connections:
    connections[uid] = {
      'closed': False,
      'procpid': calc_procpid(uid),
      'lines': [],
    }
  if closed is not None:
    connections[uid]['closed'] = closed
  connections[uid]['lines'].append(line)


with open(sys.argv[1], 'r') as f:
  previous_uid = None
  for line in f.readlines():
    parsed = parse_line(line)
    if parsed:
      add_line(**parsed)
    elif previous_uid:
      add_line(uid=previous_uid, line=line)


open_connections = {k: v for k, v in connections.items() if not v['closed']}

n = 20
o = len(open_connections)
t = len(connections)
c = t-o
print """There were {t} connections in total, {c} were closed.  The following {o} connections have
not closed, and here are (up to) their last {n} lines""".format(t=t, c=c, o=o, n=n)
for uid, connection in open_connections.items():
  print '\n\n######################################'
  lines = connection['lines']
  print '{} {}   {} lines'.format(uid, connection['procpid'], len(lines))
  print ''.join(lines[-n:])

#import ipdb; ipdb.set_trace()
