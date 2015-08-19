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



build_tarball() {
  ########################################################################
  # Copy files into the community build.
  ########################################################################
  printHeader "Creating Replicator release"
  reldir=${BUILD_DIR}/${relname}
  if [ -d $reldir ]; then
    echo "### Removing old release directory"
    \rm -rf $reldir
  fi
  
  echo "### Creating release: $reldir"
  mkdir -p $reldir
  
  # Copy everything!
  doCopy tungsten-replicator $source_replicator/build/tungsten-replicator $reldir
  cp LICENSE $reldir
  cp extra/README $reldir
  cp extra/open_source_licenses.txt $reldir
  
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
  cp -r $extra_cluster_home/conf/* $cluster_home/conf
  
  echo "# Copying cluser-home bin scripts"
  cp -r $extra_cluster_home/bin $cluster_home
  
  echo "# Copying in Ruby configuration libraries"
  cp -r $extra_cluster_home/lib $cluster_home
  cp -r $extra_cluster_home/samples $cluster_home
  
  echo "# Copying in oss-commons libraries"
  cp -r $jars_commons/* $cluster_home/lib
  cp -r $lib_commons/* $cluster_home/lib
  
  echo "### Creating tools"
  tools_dir=$reldir/tools
  mkdir -p $tools_dir
  cp $extra_tools/tpm $tools_dir
  rsync -Ca $extra_tools/ruby-tpm $tools_dir
  
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
  echo -n "GIT_URL: " >> $manifest
  git config --get remote.origin.url  >> $manifest
  echo -n "GIT_BRANCH: " >> $manifest
  git rev-parse --abbrev-ref HEAD >> $manifest
  echo -n "GIT_REVISION: " >> $manifest
  git rev-parse HEAD >> $manifest

  ########################################################################
  # Create JSON manifest file.
  ########################################################################

  # Extract revision number from the source control info.
  extractRevision() {
    (cd $1; git rev-parse HEAD)
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

  # Git repo details.
  echo    "  \"git\":" >> $manifestJSON
  echo    "  {" >> $manifestJSON
  echo    "    \"URL\": \"`git config --get remote.origin.url`\"," >> $manifestJSON
  echo    "    \"branch\": \"`git rev-parse --abbrev-ref HEAD`\"," >> $manifestJSON
  echo    "    \"revision\": \"`git rev-parse HEAD`\"" >> $manifestJSON
  echo    "  }" >> $manifestJSON
  echo    "}" >> $manifestJSON
  
  
  ########################################################################
  # Create the bash auto-completion file 
  ########################################################################
  $reldir/tools/tpm write-completion
  
  cat $manifest
  
  echo "### Cleaning up left over files"
  # find and delete directories named .svn and any file named *<sed extension>
  find ${reldir} \( -name '.svn' -a -type d -o -name "*$SEDBAK_EXT" \) -exec \rm -rf {} \; > /dev/null 2>&1
  
  ########################################################################
  # Generate tar file.
  ########################################################################
  rel_tgz=${relname}.tar.gz
  echo "### Creating tar file: ${rel_tgz}"
  (cd ${reldir}/..; tar -czf ${rel_tgz} ${relname})
}
