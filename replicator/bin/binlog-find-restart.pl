#!/usr/bin/perl
#
# VMware Continuent Tungsten Replicator
# Copyright (C) 2009-2015 VMware, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Initial developer(s): Robert Hodges
# Contributor(s): 

# TITLE: binlog-find-offset.pl
#
# DESCRIPTION:  Simple utility to find the offset in the binlog of a particular
# transaction when breadcrumbs are enabled.  To use this you must enable 
# breadcrumbs as described in the replicator breadcrumbs.js filter. 
#
# To run this utility mysqlbinlog must in the path.  Here is an to find the 
# 15th transaction after breadcrumb counter value 322 written on server id
# 16. The breadcrumbs table fully qualified name is test.breadcrumbs.
#
#   ./binlog-find-offset.pl -d /var/lib/mysql -l mysql-bin -s 16 
#     -t test.breadcrumbs -c 322 -o 15
#
# The current version of this script searches backwards through the binlogs
# for restart points, which is very fast.  However, it can miss the restart 
# point if the breadcrumb value is at the beginning of one file but the 
# transaction number we are looking for (i.e., the offset) is in the next
# file. 
use strict;
use Getopt::Std;
use File::Basename;

########################################################
# Main script
########################################################

sub usage() {
  print "Usage: $0 -t tablename -c counter -o offset -s server_id [other options or -h]\n";
  print "Required Options:\n";
  print "  -t : Breadcrumb table name in form schema.table\n";
  print "  -s : Server ID\n";
  print "  -c : Breadcrumb counter value\n";
  print "  -o : Transaction offset (0=breadcrumb update itself)\n";
  print "Other Options:\n";
  print "  -d : MySQL datadir (default: /var/lib/mysql)\n";
  print "  -l : MySQL log-bin prefix (default: mysql-bin)\n";
  print "  -h : Print help and exit\n";
  print "  -q : Suppress excess output\n";
  print "  -v : Print verbosely for debugging\n";
}

# Process command line options. 
my %options = ();
my ($BREADCRUMB_TABLE, $SERVER_ID, $COUNTER, $OFFSET);
my ($VERBOSE, $QUIET, $USAGE) = (0, 0, 0,);
my $DATADIR = "/var/lib/mysql";
my $LOG_BIN = "mysql-bin";
if (getopts("d:l:t:c:o:s:vqh",\%options)) {
  $DATADIR = $options{'d'} if (defined $options{'d'}); 
  $LOG_BIN = $options{'l'} if (defined $options{'l'}); 
  $BREADCRUMB_TABLE = $options{'t'} if (defined $options{'t'}); 
  $COUNTER = $options{'c'} if (defined $options{'c'}); 
  $OFFSET = $options{'o'} if (defined $options{'o'}); 
  $SERVER_ID = $options{'s'} if (defined $options{'s'}); 
  $VERBOSE = 1 if (defined $options{'v'});
  $QUIET = 1 if (defined $options{'q'});
  $USAGE = 1 if (defined $options{'h'});
}
else {
  usage();
  exit 1;
}

if ($USAGE) {
  usage();
  exit 0;
}

# Validate options. 
unless (defined $BREADCRUMB_TABLE) {
  print "You must provide the fully qualified breadcrumb table name using -b\n";
  exit 1;
}
unless (defined $COUNTER) {
  print "You must provide the breadcrumb counter value using -c\n";
  exit 1;
}
unless (defined $OFFSET) {
  print "You must provide the breadcrumb transaction offset using -o\n";
  exit 1;
}
unless (defined $OFFSET) {
  print "You must provide the MySQL server ID using -s\n";
  exit 1;
}
unless (-d $DATADIR and -r $DATADIR) {
  print "Data directory does not exist or is unreadable: $DATADIR\n";
  exit 1;
}

# Create the list of binlog files sorted into reverse order. 
my $file_list = `ls $DATADIR/$LOG_BIN* |grep -v index |sort -r`;
chomp $file_list;
print "### Binlog file list: $file_list\n" if $VERBOSE;
my @binlogs = split(/\s/, $file_list);
my $binlogs_size = scalar @binlogs;
print "### Size of list: $binlogs_size\n" if $VERBOSE;

unless ($binlogs_size > 0) {
  print "No binlogs found with prefix $DATADIR/$LOG_BIN\n";
  exit 1;
}

# Print the search criteria. 
print "===================================\n";
print "| BREADCRUMB SEARCH CRITERIA      |\n";
print "===================================\n";
print "Binlog directory: $DATADIR\n";
print "Binlog template : $LOG_BIN\n";
print "Breadcrumb table: $BREADCRUMB_TABLE\n";
print "Server ID       : $SERVER_ID\n";
print "Counter value   : $COUNTER\n";
print "Xact number     : $OFFSET\n";

# Now begin to parse binlogs starting at the next binlog file.  Following
# variables are what we are seeking. 
print "===================================\n";
print "| BREADCRUMB SEARCH PROCESSING    |\n";
print "===================================\n";
my $found = 0;
my $error = 0;
my $target_binlog;
my $target_offset;

# Variables used for search. 
#my $BINLOG;

my $lines = 0;
my $last_counter = -1;
my $last_offset = -1;
my $last_binlog;
my $server_id;
my $end_log_pos = 0;

foreach my $binlog (@binlogs) {
  # Open binlog file. 
  $last_binlog = $binlog;
  my $command = "mysqlbinlog --verbose $last_binlog";
  print "Searching binlog file: $last_binlog\n";
  print "Opening binlog, command: $command\n" if $VERBOSE;
  open BINLOG, "$command|" or die "Unable to read binlog file using command: $command";

  # State machine flags. 
  my $query_state = 'none';
  $last_counter = -1;
  $last_offset = -1;

  while (<BINLOG>) {
    chomp;
    $lines++;
    print "=== Line: $_\n" if $VERBOSE;

    # Look for start of next event. 
    if (/^#[0-9]+\s+([0-9]+:[0-9]+:[0-9]+) server id\s+([0-9]+)\s+end_log_pos\s+([0-9]+)\s+(\w+)/) {
      my $time = $1;
      $server_id = $2;
      $end_log_pos = $3;
      my $event = $4;
      print "### Event: $event  Server ID: $server_id  Time: $time  Log Pos: $end_log_pos\n" if $VERBOSE;

      # Look for Xid and Update_rows events. 
      if ($event eq "Update_rows" and $server_id eq $SERVER_ID) {
        $query_state = 'update_rows';
        print "### Found Update_rows\n" if $VERBOSE;
      }
      elsif ($event eq "Xid" and $server_id eq $SERVER_ID) {
        $query_state = 'xid';
        $last_offset++;
        print "### Found end of Xact: counter=$last_counter offset=$last_offset\n" if $VERBOSE;

        # At this point we can test for whether we found something. 
        if ($last_counter == $COUNTER and $last_offset == $OFFSET)
        {
           print "Found restart point: counter=$last_counter offset=$last_offset end_log_pos=$end_log_pos\n";
           $found = 1;
           last;
        }
        elsif ($last_counter > $COUNTER) 
        {
           print "  (Restart point too high: counter=$last_counter offset=$last_offset end_log_pos=$end_log_pos)\n";
           last;
        }
      }
      else
      {
        $query_state = "none";
        print "### Found non-matching event: counter=$last_counter offset=$last_offset\n" if $VERBOSE;
      }
    }
    # If we are in the update seek state, look for the query. 
    elsif ($query_state eq 'update_rows') {
      if (/update $BREADCRUMB_TABLE/i) {
        $query_state = 'breadcrumbs';
        print "### Found Breadcrumbs update" if $VERBOSE;
      } 
    }
    elsif ($query_state eq 'breadcrumbs') {
      if (/###   \@2=([0-9]+)/i) {
        my $counter = $1;

        # Make sure that we don't overshoot due to an invalid counter/offset 
        # combination. 
        if ($last_counter == $COUNTER and $last_offset < $OFFSET) {
          $error = 1;
          print "ERROR!! Found new counter before reaching offset for current counter\n";
          print "last counter=$last_counter last offset=$last_offset current end_log_pos=$end_log_pos\n";
          last;
        }
        $last_counter = $counter;
        $last_offset = -1;
        print "### Found Breadcrumbs counter: $last_counter\n" if $VERBOSE;
      } 
    }
  }
 
  # Close and proceed to next binlog. 
  close(BINLOG);

  # If we found a value exit here. 
  if ($found) {
    print "### Found offset, leaving loop\n" if $VERBOSE;
    last;
  }
  elsif ($error) {
    print "Aborting search\n";
    last;
  }
}

print "===================================\n";
print "| RESTART POSITION RESULT         |\n";
print "===================================\n";
if ($found) {
  print "Binlog file      : $last_binlog\n";
  print "Binlog offset    : $end_log_pos\n";
  my $short_binlog = basename($last_binlog);
  my $event_id = "$short_binlog:$end_log_pos";
  print "Tungsten event ID: $event_id\n";
  print "Online command   : trepctl online -from-event $event_id\n";
  exit 0;
} 
else {
  print "Not found! :(\n";
  exit 1;
}
