#!/bin/bash

##########################################################################
# NAME:  BUILD
#
# SUMMARY:  Build script for Tungsten Replicator
#
# OVERVIEW:
#   This script performs a full build of the replicator and all its
#   dependencies
#
#   Before building you should review properties in the 'config' file. If
#   you need to override them, do so in config.local (which is svn:ignored)
#   then run the script.
#
#   You can specify a different configuration file as an argument.
#   Otherwise, it defaults to "config" file.
#
# USAGE:
#    build [config-file]
#
##########################################################################

##########################################################################
# Source and define various functions
##########################################################################

cd `dirname $0`
source ./helpers.sh

# Creates the src tarball
build_source() {
  printHeader "Creating Replicator source release"

  # Copy sources to appropriate directory.  Tests are not included, so we
  # drop them.
  reldir_src=${reldir}-src
  echo "### Creating source release directory: ${reldir_src}"
  rm -rf ${reldir_src}
  mkdir -p ${reldir_src}

  echo "### Copying in source files"
  modules_sources_folder=$reldir_src/sources
  builder_folder=$reldir_src/builder
  mkdir $modules_sources_folder
  mkdir $builder_folder
  cp -r $SRC_DIR/replicator $modules_sources_folder
  cp -r $SRC_DIR/commons $modules_sources_folder
  cp -r build.sh helpers.sh build_tarball.sh config extra $builder_folder/
  echo "SKIP_CHECKOUT=1" > $builder_folder/config.local

  # Clean all copied source trees to keep only necessary files
  echo "### Cleaning-up source folders"
  reldir_src_sources=${reldir_src}/sources
  doAnt commons ${reldir_src_sources}/commons/build.xml clean
  doAnt replicator ${reldir_src_sources}/replicator/build.xml clean

  # Remove svn folders from source distrib
  echo "### Cleaning-up extra svn information"
  find $reldir_src -name ".svn" -exec \rm -rf {} \; > /dev/null 2>&1

  echo "### Copying-in manifest"
  # Use the same manifest as for bin distrib
  cp $manifest $reldir_src/

  rel_src_tgz=${relname}-src.tar.gz

  echo "### generating source tar file: ${rel_src_tgz}"
  (cd ${reldir}/..; tar -czf ${rel_src_tgz} ${relname}-src/)
}

##########################################################################
# Handle arguments.
##########################################################################

if [ ! -z $1 ]; then
  if [ ! -r $1 ]; then
    echo "!!! Unknown or unreadable configuration file: $1"
    exit 1
  fi
  config=$1
fi

##########################################################################
# Initialization and cautions to user.
##########################################################################

if [ -z $config ]; then
  config=config
fi

printHeader "REPLICATOR BUILD SCRIPT"

#if [ -n "$SKIP_PROMPT" ]
#then
#    echo "Skipping confirmation, as the variable SKIP_PROMPT is set"
#else
#    echo "Did you update config.local? (press enter to continue)"
#    read ignored_answer
#fi

source ./$config

if [ -f config.local ]; then
  echo "Overriding $config with config.local"
  source config.local
fi

##########################################################################
# Set global variables.
##########################################################################

# Source root is parent directory. 
SRC_DIR=..
source_commons=${SRC_DIR}/commons
source_replicator=${SRC_DIR}/replicator
source_community_extra=extra

extra_replicator=${source_community_extra}/replicator
extra_cluster_home=${source_community_extra}/cluster-home
extra_tools=${source_community_extra}/tools

jars_commons=${source_commons}/build/jars
lib_commons=${source_commons}/lib

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

##########################################################################
# Environment checks.
##########################################################################
echo "### Checking environment..."
checkCommandExists ant
echo "### ... Success!"


##########################################################################
# Additional initializations
##########################################################################

# Release name.
product="Tungsten Replicator"
relname=tungsten-replicator-${VERSION}
# Add Jenkins build number if any
if [ "x${BUILD_NUMBER}" != "x" ]
then
    relname=${relname}-${BUILD_NUMBER}
fi

##########################################################################
# Build products.
##########################################################################

if [ ${SKIP_BUILD} -eq 1 ]
then
  printHeader "Using existing builds"
else
  printHeader "Building replicator from source"
  # Run the builds.
  doAnt commons $source_commons/build.xml clean dist 
  doAnt replicator $source_replicator/build.xml clean dist javadoc
fi

source ./build_tarball.sh
build_tarball

##########################################################################
# Create source build if desired.
##########################################################################

if [ "$SKIP_SOURCE" = 0 ]; then
  build_source
else
  echo "### Skipping source code release generation"
fi
