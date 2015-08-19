#!/bin/bash

##########################################################################
# NAME:  BUILD_TARBALL
#
# SUMMARY:  Creates the release tarball of Tungsten Replicator from
#           compiled sources
#
# OVERVIEW: Copies required files in the 'release directory', creates the
#           cluster-home plus manifest files and generates the tarball
#
# USAGE:
#    First check out compile all required sources. The include this file
#    (source build_tarball.sh) and call 'build_tarball' function. Note
#    that all variables must be set appropriately prior to calling the
#    function (see build.sh and config files for more details)
#
##########################################################################
GIT=/build/toolchain/lin32/git-1.7.11/bin/git
RSYNC=/build/toolchain/lin32/rsync-3.0.7/bin/rsync
SED=/build/toolchain/lin32/sed-4.1.5/bin/sed
TAR=/build/toolchain/lin32/tar-1.23/bin/tar
GZIP=/build/toolchain/lin32/gzip-1.3.5/bin/gzip

source config
source helpers.sh

  ########################################################################
  # Copy files into the community build.
  ########################################################################
  printHeader "Creating Replicator release"
  relname='tungsten-replicator-4.0.0'
  reldir=${BUILD_DIR}/${relname}
  if [ -d $reldir ]; then
    echo "### Removing old release directory"
    \rm -rf $reldir
  fi

  echo "### Creating release: $reldir"
  mkdir -p $reldir

  # Copy everything!
  doCopy tungsten-replicator ../replicator//build/tungsten-replicator $reldir
  doCopy bristlecone ../bristlecone/build/dist/bristlecone $reldir
  doCopy cookbook ../cookbook $reldir
  cp LICENSE $reldir
  cp extra/README $reldir
  cp extra/README.LICENSES $reldir

  ########################################################################
  # Fix up replicator files.
  ########################################################################

  reldir_replicator=$reldir/tungsten-replicator
  replicator_bin=$reldir_replicator/bin/replicator

  ########################################################################
  # Create the cluster home.
  ########################################################################

  echo "### Creating cluster home"
  cluster_home=$reldir/cluster-home
  mkdir -p $cluster_home/conf/cluster
	# log directory for cluster-home/bin programs
  mkdir -p $cluster_home/log

  echo "# Copying cluser-home/conf files"
  cp -r  extra/cluster-home/conf/* $cluster_home/conf

  echo "# Copying cluser-home bin scripts"
  cp -r extra/cluster-home/bin $cluster_home

  echo "# Copying in Ruby configuration libraries"
  cp -r extra/cluster-home/lib $cluster_home
  cp -r extra/cluster-home/samples $cluster_home

  echo "# Copying in oss-commons libraries"
  cp -r ../commons/build/jars/* $cluster_home/lib
  cp -r ../commons/lib/* $cluster_home/lib

  echo "### Creating tools"
  tools_dir=$reldir/tools
  mkdir -p $tools_dir
  cp extra/tools/tpm $tools_dir
  $RSYNC -Ca extra/tools/ruby-tpm $tools_dir

  echo "### Deleting duplicate librairies in Bristlecone ###"
  echo "# tungsten-common"
  rm -vf $reldir/bristlecone/lib/tungsten-common*.jar

  ########################################################################
  # Create manifest file.
  ########################################################################

  manifest=${reldir}/.manifest
  echo "### Creating manifest file: $manifest"

  echo "# Build manifest file" >> $manifest
  echo "DATE: `date`" >> $manifest
  echo "RELEASE: ${relname}" >> $manifest
  echo "USER ACCOUNT: ${USER}" >> $manifest

  # Hudson environment values.  These will be empty in local builds.
  echo "BUILD_NUMBER: ${BUILD_NUMBER}" >> $manifest
  echo "BUILD_ID: ${BUILD_NUMBER}" >> $manifest
  echo "JOB_NAME: ${JOB_NAME}" >> $manifest
  echo "BUILD_TAG: ${BUILD_TAG}" >> $manifest
  echo "HUDSON_URL: ${HUDSON_URL}" >> $manifest
  echo "SVN_REVISION: ${SVN_REVISION}" >> $manifest

  # Local values.
  echo "HOST: `hostname`" >> $manifest
  echo "SVN URLs:" >> $manifest
  echo "  ${TCOM_SVN_URL}" >> $manifest
  echo "  ${TREP_SVN_URL}" >> $manifest
  #echo "  ${TREP_EXT_SVN_URL}" >> $manifest
  echo "  ${BRI_SVN_URL}" >> $manifest
  echo "  ${COOK_SVN_URL}" >> $manifest

  echo "SVN Revisions:" >> $manifest
  echo -n "  commons: " >> $manifest
  $GIT log --format="%H" -n 1 >> $manifest
  echo -n "  replicator: " >> $manifest
  $GIT log --format="%H" -n 1 >> $manifest
  echo -n "  bristlecone: " >> $manifest
  $GIT log --format="%H" -n 1 >> $manifest
  echo -n "  cookbook: " >> $manifest
  $GIT log --format="%H" -n 1 >> $manifest

  ########################################################################
  # Create JSON manifest file.
  ########################################################################

  # Extract SVN revision number from the SVN info.
  extractRevision() {
    $GIT log --format="%H" -n 1
  }

  manifestJSON=${reldir}/.manifest.json
  echo "### Creating JSON manifest file: $manifestJSON"

  # Local details.
  echo    "{" >> $manifestJSON
  echo    "  \"date\": \"`date`\"," >> $manifestJSON
  echo    "  \"product\": \"${product}\"," >> $manifestJSON
  echo    "  \"version\":" >> $manifestJSON
  echo    "  {" >> $manifestJSON
  echo    "    \"major\": ${VERSION_MAJOR}," >> $manifestJSON
  echo    "    \"minor\": ${VERSION_MINOR}," >> $manifestJSON
  echo    "    \"revision\": ${VERSION_REVISION}" >> $manifestJSON
  echo    "  }," >> $manifestJSON
  echo    "  \"userAccount\": \"${USER}\"," >> $manifestJSON
  echo    "  \"host\": \"`hostname`\"," >> $manifestJSON

  # Hudson environment values.  These will be empty in local builds.
  echo    "  \"hudson\":" >> $manifestJSON
  echo    "  {" >> $manifestJSON
  echo    "    \"buildNumber\": ${BUILD_NUMBER-null}," >> $manifestJSON
  echo    "    \"buildId\": ${BUILD_NUMBER-null}," >> $manifestJSON
  echo    "    \"jobName\": \"${JOB_NAME}\"," >> $manifestJSON
  echo    "    \"buildTag\": \"${BUILD_TAG}\"," >> $manifestJSON
  echo    "    \"URL\": \"${HUDSON_URL}\"," >> $manifestJSON
  echo    "    \"SVNRevision\": ${SVN_REVISION-null}" >> $manifestJSON
  echo    "  }," >> $manifestJSON

  # SVN details.
  echo    "  \"SVN\":" >> $manifestJSON
  echo    "  {" >> $manifestJSON
  echo    "    \"commons\":" >> $manifestJSON
  echo    "    {" >> $manifestJSON
  echo    "      \"URL\": \"${TCOM_SVN_URL}\"," >> $manifestJSON
  echo -n "      \"revision\": " >> $manifestJSON
  echo           "\"`extractRevision $source_commons`\"" >> $manifestJSON
  echo    "    }," >> $manifestJSON
  echo    "    \"replicator\":" >> $manifestJSON
  echo    "    {" >> $manifestJSON
  echo    "      \"URL\": \"${TREP_SVN_URL}\"," >> $manifestJSON
  echo -n "      \"revision\": " >> $manifestJSON
  echo           "\"`extractRevision $source_replicator`\"" >> $manifestJSON
  echo    "    }," >> $manifestJSON
  echo    "    \"bristlecone\":" >> $manifestJSON
  echo    "    {" >> $manifestJSON
  echo    "      \"URL\": \"${BRI_SVN_URL}\"," >> $manifestJSON
  echo -n "      \"revision\": " >> $manifestJSON
  echo           "\"`extractRevision $source_bristlecone`\"" >> $manifestJSON
  echo    "    }," >> $manifestJSON
  echo    "    \"cookbook\":" >> $manifestJSON
  echo    "    {" >> $manifestJSON
  echo    "      \"URL\": \"${COOK_SVN_URL}\"," >> $manifestJSON
  echo -n "      \"revision\": " >> $manifestJSON
  echo           "\"`extractRevision $source_cookbook`\"" >> $manifestJSON
  echo    "    }" >> $manifestJSON
  echo    "  }" >> $manifestJSON
  echo    "}" >> $manifestJSON


  ########################################################################
  # Create the bash auto-completion file
  ########################################################################
#  $reldir/tools/tpm write-completion

  cat $manifest

  echo "### Cleaning up left over files"
  # find and delete directories named .svn and any file named *<sed extension>
  find ${reldir} \( -name '.git' -a -type d -o -name "*$SEDBAK_EXT" \) -exec \rm -rf {} \; > /dev/null 2>&1
echo "### Copying DLF"
cp -r DLF $reldir
cp -r EULA $reldir
cp -r OSS $reldir

  ########################################################################
  # Generate tar file.
  ########################################################################
set -x
  rel_tgz=${relname}.tar
  echo "### Creating tar file: ${rel_tgz}"
  (cd ${reldir}/..; $TAR -cf ${rel_tgz} ${relname})
  cp build/${rel_tgz} ../../../../publish
  $GZIP ../../../../publish/${rel_tgz}
set +x

