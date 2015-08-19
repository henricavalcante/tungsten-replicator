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

# TITLE: analyze-binlog.pl
#
# DESCRIPTION:  Simple utility to print stats on MySQL binlogs files.  You 
# must run mysqlbinlog to create a text representation of the binlog file 
# as shown in the following example: 
#
#   mysqlbinlog mysql-bin.001248 | ./analyze-binlog -q
#
# OPTIONS:
#   -q : Run quietly with minimal output (good for crunching lots of files)
#   -v : Print ridiculous amounts of information to help debug the script
#   -h : Print usage. 

use strict;
use Getopt::Std;

########################################################
# Time conversion utility functions. 
########################################################

# Convert time string to seconds.  
sub timeToSeconds($) {
  my ($t) = @_;
  $t =~ /([0-9]+):([0-9]+):([0-9]+)/;
  my $seconds = ($1 * 3600) + ($2 * 60) + $3;
  return $seconds;
}

# Convert seconds to time string. 
sub secondsToTime($) {
  my ($seconds) = @_;
  my $s = $seconds % 60;
  $seconds = int($seconds / 60);
  my $m = $seconds % 60;
  $seconds = int($seconds / 60);
  my $h = $seconds % 60;
  my $t = sprintf("%d:%2.2d:%2.2d", $h, $m, $s);
  return $t;
}

########################################################
# Main script
########################################################

sub usage() {
  print "Usage: mysqlbinlog binlog_file | $0 [-h] [-q] [-v]\n";
  print "Options:\n";
  print "  -h : Print help\n";
  print "  -q : Suppress excess output\n";
  print "  -v : Print verbosely for debugging\n";
}

# Process command line options. 
my %options = ();
my ($VERBOSE, $QUIET, $USAGE) = (0, 0, 0,);
if (getopts("vqh",\%options)) {
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

# Basic data. 
my $binlog_version = "???";
my $server_version = "???";
my $lines = 0;
my $events = 0;

my %event_counts;
unless ($QUIET) {
  # MySQL binlog event type counters. 
  $event_counts{"Start_v3"} = 0;
  $event_counts{"Stop"} = 0;
  $event_counts{"Query"} = 0;
  $event_counts{"Rotate"} = 0;
  $event_counts{"Intvar"} = 0;
  $event_counts{"Load"} = 0;
  $event_counts{"New_load"} = 0;
  $event_counts{"Slave"} = 0;
  $event_counts{"Create_file"} = 0;
  $event_counts{"Append_block"} = 0;
  $event_counts{"Delete_file"} = 0;
  $event_counts{"Exec_load"} = 0;
  $event_counts{"RAND"} = 0;
  $event_counts{"Xid"} = 0;
  $event_counts{"User var"} = 0;
  $event_counts{"Format_desc"} = 0;
  $event_counts{"Table_map"} = 0;
  $event_counts{"Write_rows_event_old"} = 0;
  $event_counts{"Update_rows_event_old"} = 0;
  $event_counts{"Delete_rows_event_old"} = 0;
  $event_counts{"Write_rows"} = 0;
  $event_counts{"Update_rows"} = 0;
  $event_counts{"Delete_rows"} = 0;
  $event_counts{"Begin_load_query"} = 0;
  $event_counts{"Execute_load_query"} = 0;
  $event_counts{"Incident"} = 0;
}

# SQL statement counts. 
my %sql_counts;
$sql_counts{"begin"} = 0;
$sql_counts{"rollback"} = 0;
$sql_counts{"insert into"} = 0;
$sql_counts{"select into"} = 0;
$sql_counts{"insert"} = 0;
$sql_counts{"update"} = 0;
$sql_counts{"delete"} = 0;
$sql_counts{"alter table"} = 0;
$sql_counts{"drop table"} = 0;
$sql_counts{"create table"} = 0;
$sql_counts{"create temp table"} = 0;

# Statistical counters. 
my $start_time_secs = 0;
my $max_events_per_sec = 0;
my $max_event_len = 0;
my $max_bytes_per_sec = 0;
my $max_queries_per_sec = 0;
my $max_xacts_per_sec = 0;
my $max_events_time; 
my $max_event_len_time; 
my $max_bytes_time; 
my $max_queries_time; 
my $max_xacts_time; 
my $last_end_log_pos = 0;

# State machine flags. 
my $query_state = 'none';

# Variables used for counts. 
my $time;
my $end_log_pos;
my $event;
my $current_time;
my $start_log_pos;
my $last_log_pos;
my $last_end_log_pos;

# Local stat counts. 
my $events_at_this_time;
my $bytes_at_this_time;
my $queries_at_this_time;
my $xacts_at_this_time;

while (<>) {
  # Increment line counter. 
  $lines++; 
  chomp;
  print "### Line: $_\n" if $VERBOSE;
  unless ($QUIET) {
    print "On line: $lines, on event: $events\n" if ($lines % 100000 == 0);
  }

  # Look for start of next event. 
  if (/^#[0-9]+\s+([0-9]+:[0-9]+:[0-9]+) server.* end_log_pos\s+([0-9]+)\s+(\w+)/) {
    $events++;
    $time = $1;
    $end_log_pos = $2;
    $event = $3;
    print "### Event: $event  Time: $time  Log Pos: $end_log_pos\n" if $VERBOSE;

    # Record the time. 
    if ($start_time_secs == 0) {
      $start_time_secs = timeToSeconds($time);
    }

    # Record event count. 
    if (defined $event_counts{$event}) {
      $event_counts{$event}++;
    }
    else {
      $event_counts{$event} = 1;
    }

    # Initialize time counter if necessary. 
    if (! defined $current_time) {
      $current_time = $time; 
      $events_at_this_time = 0;
      $bytes_at_this_time = 0;
      $queries_at_this_time = 0;
      $xacts_at_this_time = 0;
      $start_log_pos = 0;
      $last_log_pos = 0;
      $last_end_log_pos = 0;
    }

    # If we are on a new time, check and reset per-second counters. 
    if ($time ne $current_time) {
      # See if we have the maximum events per second. 
      if ($events_at_this_time > $max_events_per_sec) {
        $max_events_per_sec = $events_at_this_time;
        $max_events_time = $current_time;
        print "New max events per second: $max_events_per_sec @ $max_events_time\n" if $VERBOSE;
      }

      # Check to see if we have maximum bytes per second.
      if ($bytes_at_this_time > $max_bytes_per_sec) {
        $max_bytes_per_sec = $bytes_at_this_time;
        $max_bytes_time = $current_time;
        print "New max bytes per second: $max_bytes_per_sec\n" if $VERBOSE;
      }

      # Check to see if we have maximum queries per second.
      if ($queries_at_this_time > $max_queries_per_sec) {
        $max_queries_per_sec = $queries_at_this_time;
        $max_queries_time = $current_time;
        print "New max queries per second: $max_queries_per_sec\n" if $VERBOSE;
      }

      # Check to see if we have maximum transactions per second.
      if ($xacts_at_this_time > $max_xacts_per_sec) {
        $max_xacts_per_sec = $xacts_at_this_time;
        $max_xacts_time = $current_time;
        print "New max xacts per second: $max_xacts_per_sec\n" if $VERBOSE;
      }

      # Reset counters.  
      $current_time = $time; 
      $events_at_this_time = 0;
      $queries_at_this_time = 0;
      $xacts_at_this_time = 0;
      $start_log_pos = $last_log_pos;
    }

    # Update per_second counters. 
    $events_at_this_time++;
    $bytes_at_this_time = $end_log_pos - $start_log_pos;
    $last_log_pos = $end_log_pos;
    $query_state = 'none';

    # Record event length and check for largest event so far. 
    my $this_event_len = $end_log_pos - $last_end_log_pos; 
    $last_end_log_pos = $end_log_pos; 
    if ($this_event_len > $max_event_len) {
      $max_event_len = $this_event_len;
      $max_event_len_time = $current_time;
      print "New max event length: $max_event_len\n" if $VERBOSE;
    }

    # If this is the start event, get the binlog and server version
    if ($event eq "Query") {
      $queries_at_this_time++;
      $query_state = 'query_seek'
    }
    elsif ($event eq "Xid") {
      $xacts_at_this_time++;
    }
    elsif ($event eq "Start") {
      if (/Start: binlog v ([0-9])/) {
        $binlog_version = $1;
      }
      if (/server v ([0-9\.]+)/) {
        $server_version = $1;
      }
    }
  }
  # Assemble SQL query statistics.  These are approximate.  
  elsif ($query_state eq 'query_seek') {
    if (/(begin)/i) {
      $sql_counts{'begin'}++;
      $query_state = 'none';
    } 
    if (/(rollback)/i) {
      $sql_counts{'rollback'}++;
      $query_state = 'none';
    } 
    elsif (/(select\s+into)/i) {
      $sql_counts{'select into'}++;
      $query_state = 'none';
    } 
    elsif (/(insert\s+into)/i) {
      $sql_counts{'insert into'}++;
      $query_state = 'none';
    } 
    elsif (/(insert)/i) {
      $sql_counts{'insert'}++;
      $query_state = 'none';
    } 
    elsif (/(update)/i) {
      $sql_counts{'update'}++;
      $query_state = 'none';
    } 
    elsif (/(delete)/i) {
      $sql_counts{'delete'}++;
      $query_state = 'none';
    } 
    elsif (/(truncate)/i) {
      $sql_counts{'truncate'}++;
      $query_state = 'none';
    } 
    elsif (/(drop\s+table)/i) {
      $sql_counts{'drop table'}++;
      $query_state = 'none';
    } 
    elsif (/(alter\s+table)/i) {
      $sql_counts{'alter table'}++;
      $query_state = 'none';
    } 
    elsif (/(create\s+table)/i) {
      $sql_counts{'create table'}++;
      $query_state = 'none';
    } 
    elsif (/(create\s+temporary\s+table)/i) {
      $sql_counts{'create temporary table'}++;
      $query_state = 'none';
    } 
  }
}

# Compute duration and average time.  Deal with times that role over midnight. 
my $end_time_secs = timeToSeconds($time);
if ($start_time_secs > $end_time_secs) {
  $end_time_secs += (24 * 3600);
}
my $interval_secs  = $end_time_secs - $start_time_secs;
my $interval_time  = secondsToTime($interval_secs);
my $events_per_sec       = $events / $interval_secs;
my $bytes_per_sec        = $end_log_pos / $interval_secs;
my $query_events_per_sec = $event_counts{'Query'} / $interval_secs;
my $xact_events_per_sec  = $event_counts{'Xid'} / $interval_secs;

# Print totals. 
print "===================================\n";
print "| SUMMARY INFORMATION             |\n";
print "===================================\n";
print "Server Version    : $server_version\n";
print "Binlog Version    : $binlog_version\n";
print "Duration          : $interval_time (${interval_secs}s)\n";
printf "Output Lines Read : %d\n", $lines;

print "\n===================================\n";
print "| SUMMARY STATISTICS              |\n";
print "===================================\n";
printf "Events            : %15d\n", $events;
printf "Bytes             : %15d\n", $end_log_pos;
printf "Queries           : %15d\n", $event_counts{'Query'};
printf "Xacts             : %15d\n", $event_counts{'Xid'};
printf "Max. Events/Sec.  : %15d @%s\n", $max_events_per_sec, $max_events_time;
printf "Max. Bytes/Sec.   : %15d @%s\n", $max_bytes_per_sec, $max_bytes_time;;
printf "Max. Queries/Sec. : %15d @%s\n", $max_queries_per_sec, 
  $max_queries_time;
printf "Max. Xacts/Sec.   : %15d @%s\n", $max_xacts_per_sec, $max_xacts_time;
printf "Max. Event Bytes  : %15d @%s\n", $max_event_len, $max_event_len_time;
printf "Avg. Events/Sec.  : %18.2f\n", $events_per_sec;
printf "Avg. Bytes/Sec.   : %18.2f\n", $bytes_per_sec;
printf "Avg. Queries/Sec. : %18.2f\n", $query_events_per_sec;
printf "Avg. Xacts/Sec.   : %18.2f\n", $xact_events_per_sec;

print "\n===================================\n";
print "| EVENT COUNTS                    |\n";
print "===================================\n";
foreach my $key (sort keys %event_counts) {
  printf "%-21.21s: %12d\n", $key, $event_counts{$key};
}

print "\n===================================\n";
print "| SQL STATEMENT COUNTS            |\n";
print "===================================\n";
foreach my $key (sort keys %sql_counts) {
  printf "%-21.21s: %12d\n", $key, $sql_counts{$key};
}

