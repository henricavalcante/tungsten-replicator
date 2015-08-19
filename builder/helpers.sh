#!/bin/bash

##########################################################################
# NAME:  HELPERS
#
# SUMMARY:  Utilities for building Tungsten Replicator
#
# USAGE:  Just include this file at the beginning of build shell scripts
#         (with 'source helpers.sh') and use the utility functions 
#         provided
#
##########################################################################


# Print a nice header for output.
printHeader() {
  echo
  echo "################################################################"
  echo "# $1"
  echo "################################################################"
  echo
}

# Check out code and exit if unsuccessful.
doSvnCheckout() {
  local component=$1
  local url=$2
  local source_dir=$3
  echo "### Checking out component: $component"
  echo "# SVN URL: $url"
  echo "# Source directory: $source_dir"
  svn checkout $url $source_dir
  if [ "$?" != "0" ]; then
    echo "!!! SVN checkout failed!"
    exit 1
  fi
}

# Run an ant build and exit if it fails.
doAnt() {
  local component=$1; shift
  local build_xml=$1; shift
  local targets=$*
  echo "### Building component: $component"
  echo "# Ant build.xml: $build_xml"
  echo "# Ant targets:   $targets"
  ant -f $build_xml $targets
  if [ "$?" != "0" ]; then
    echo "!!! Ant build failed"
    exit 1
  fi
}

# Copy from the component build location to the release.
doCopy() {
  local component=$1
  local build_src_dir=$2
  local build_tgt_dir=$3
  echo "### Copying component: $component"
  echo "# Build source: $build_src_dir"
  echo "# Build target: $build_tgt_dir"
  cp -r $build_src_dir $build_tgt_dir/$component
  if [ "$?" != "0" ]; then
    echo "!!! Copy operation failed"
    exit 1
  fi
}

# Tries to locate the command passed as first argument. If the command is not
# found, exists with a comprehensive error message
checkCommandExists() {
  which $1 > /dev/null 2>&1
  if [ $? -eq 0 ]; then
    echo "# $1 found."
  else
    echo "!!! Required $1 executable not found, please install it! Exiting."
    exit 1
  fi
}

# Removes the given 1st arg library pattern and copy from 2nd arg jar to
# either cluster-home/lib/ or to the 3rd arg dir if given
cleanupLib() {
  find ${reldir} -name $1 -exec \rm -f {} \; > /dev/null 2>&1
  if [ x$3 == "x" ]
  then
    cp $2 $cluster_home/lib
  else
    cp $2 $3
  fi
}

##########################################################################
# Handle platform differences.  This script works on MacOSX & Linux.
##########################################################################

# Prevents creation of '._' files under Max OS/X
export COPYFILE_DISABLE=true

# Fixes the problem with differing sed -i command in Linux and Mac
SEDBAK_EXT=.sedbak
if [ "`uname -s`" = "Darwin" ]
then
  SED_DASH_I="sed -i $SEDBAK_EXT"
else
  SED_DASH_I="sed -i$SEDBAK_EXT"
fi
