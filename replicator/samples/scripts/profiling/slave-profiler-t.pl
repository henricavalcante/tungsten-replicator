#!/usr/bin/perl
#
# VMware Continuent Tungsten Replicator
# Copyright (C) 2015 VMware, Inc. All rights reserved.
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

# TITLE: slave-profiler.pl
#
# DESCRIPTION:  Profiles slave status for Tungsten and MySQL replication.  
#
#   slave-profiler.pl -x {mysql|tungsten} [-t s] [-u user] [-p pw] 
#                     [-o trace.out] [-h]
#
# OPTIONS:
#   -x : Execute against 'mysql' or 'tungsten' [no default]
#   -t : How many seconds between profiling checks [default: 10]
#   -i : How many iterations [default: 0 = forever]
#   -u : MySQL user [default: none]
#   -p : MySQL password [default: none]
#   -o : Profile trace output file [default: trace.out]
#   -h : Print help

use strict;
use Getopt::Std;

########################################################
# Main script
########################################################

sub usage() {
  print "Usage: $0 -x {mysql|tungsten} options\n";
  print "Options:\n";
  print "  -x : Execute against 'mysql' or 'tungsten' [no default]\n";
  print "  -t : How many seconds between profiling checks [default: 10]\n";
  print "  -i : How many iterations [default: 0 = forever]\n";
  print "  -u : MySQL user [default: none]\n";
  print "  -p : MySQL password [default: none]\n";
  print "  -H : MySQL/tungsten host default: 127.0.0.1]\n";
  print "  -P : MySQL/tungsten port default: 3306]\n";
  print "  -o : Profile trace output file [default: trace.out]\n";
  print "  -v : Print debugging output\n";
  print "  -h : Print help\n";
}

# Process command line options. 
my %options = ();
my ($USER, $PASSWD, $HOST, $PORT, $TARGET); 
my ($TIME, $TRACE, $VERBOSE, $USAGE) = (10, "trace.out", 0, 0);
my $ITERATIONS = 0;
if (getopts("x:t:i:u:p:H:P:o:vh",\%options)) {
  $TARGET     = $options{'x'} if (defined $options{'x'});
  $TIME       = $options{'t'} if (defined $options{'t'});
  $ITERATIONS = $options{'i'} if (defined $options{'i'});
  $USER       = $options{'u'} if (defined $options{'u'});
  $PASSWD     = $options{'p'} if (defined $options{'p'});
  $HOST       = $options{'H'} if (defined $options{'H'});
  $PORT       = $options{'P'} if (defined $options{'P'});
  $TRACE      = $options{'o'} if (defined $options{'o'});
  $VERBOSE    = 1 if (defined $options{'v'});
  $USAGE      = 1 if (defined $options{'h'});
}
else {
  usage();
  exit 1;
}

# Print help if desired. 
if ($USAGE) {
  usage();
  exit 0;
}

# Validate that we got either MySQL or Tungsten. 
unless ($TARGET =~ /mysql|tungsten/) {
  print "You must select a target with -x: mysql or tungsten\n";
  usage();
  exit 1;
}

# Validate the time value. 
unless ($TIME =~ /^[0-9]+$/ and $TIME > 0) {
  print "Time value must be a positive integer: $TIME\n";
  usage();
  exit 1;
}

# Start trace file and print header. 
open (TFILE, ">>$TRACE") or die "Cannot open trace output file: $TRACE\n";
print TFILE "Datetime, Date_In_Seconds, Seconds_Behind_Master, Seconds_Changed, Total_Bytes_Processed, Bytes_Processed\n";

# Format query to get slave state. 
my $cmd;
if ($TARGET eq "mysql") {
  my $query = "show slave status\\G";
  $cmd = "mysql -e\'$query\'";
  if (defined $USER) {
    $cmd = $cmd . " -u$USER";
  }
  if (defined $PASSWD) {
    $cmd = $cmd . " -p$PASSWD";
  }
  if (defined $HOST) {
    $cmd = $cmd . " -h$HOST";
  }
  if (defined $PORT) {
    $cmd = $cmd . " -P$PORT";
  }
  print "MySQL command: $cmd\n";
}
else {
  my $trepctl = `which trepctl`;
  chomp $trepctl;
  if ($trepctl eq "") {
    print "Unable to find trepctl command in path!\n";
    exit 1;
  } 
  $cmd = "$trepctl";
  if (defined $HOST) {
    $cmd = $cmd . " -host $HOST";
  }
  if (defined $PORT) {
    $cmd = $cmd . " -port $PORT";
  }
  $cmd = $cmd . " status";
  print "Tungsten command: $cmd\n";
}

# Loop through a few times. 
my $last_seconds_behind; 
my $last_log_pos;
my $total_bytes_processed = 0;
my $iterations = 0;

while ($iterations < $ITERATIONS or $ITERATIONS == 0) {
  $iterations++;
  open(MYSQL, "$cmd |") or die "Cannot issue command: $cmd";

  # Set default values. 
  my $seconds_behind = -1;
  my $seconds_behind_changed = -1;
  my $bytes_processed = -1;
  my $datetime = `date +'%Y-%m-%d %H:%M:%S'`;
  chomp $datetime;
  my $date_in_seconds = `date +'%s'`;
  chomp $date_in_seconds;

  while (<MYSQL>) {
    if ($VERBOSE) {
      print "LINE: $_";
    }

    # Compute seconds behind and change thereon.  MySQL and Tungsten
    # regular expressions are provided. 
    if (/Seconds_Behind_Master: ([0-9]+)/ || /appliedLatency\s*: (\d+)\./) {
      $seconds_behind = $1;
      if (defined $last_seconds_behind) {
        $seconds_behind_changed = $last_seconds_behind - $seconds_behind;
      }
      $last_seconds_behind = $seconds_behind;
    }

    # Compute bytes processed and change thereon.  MySQL and Tungsten 
    # regular expressions are provided. 
    if (/Exec_Master_Log_Pos: ([0-9]+)/ || /appliedLastEventId\s*: .*\.\d+:(\d+);/) {
      my $log_pos = $1;
      if (defined $last_log_pos) {
        if ($last_log_pos > $log_pos) {
          # Log has just rotated...
          $bytes_processed = $log_pos;;
        }
        else {
          $bytes_processed = $log_pos - $last_log_pos;
        }
        $total_bytes_processed += $bytes_processed;
      }
      $last_log_pos = $log_pos;
    }
  }
  close MYSQL;
  my $rc = $?; 
 
  # Print result. 
  printf TFILE "\"%s\", %d, %d, %d, %d, %d\n", $datetime, $date_in_seconds, 
    $seconds_behind, $seconds_behind_changed, $total_bytes_processed, 
    $bytes_processed;

  # Bide a wee.
  sleep($TIME);
}

# Close trace file and finish. 
close TFILE;
print "Done!\n";
