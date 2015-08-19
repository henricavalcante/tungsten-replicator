                             Protobuf README File
                               29 December 2014
                                Robert Hodges

OVERVIEW

Tungsten Replicator uses Google Protocol Buffers (protobuf) to
serialize records in the Transaction History Log (THL).  The
definition of the log format is containined in
protobuf/TungstenProtobufMessage.

Protobuf code and documentation is available here: 

  https://developers.google.com/protocol-buffers/

Tungsten replicator code intersects with protobuf in the following
Java packages.

  com.continuent.tungsten.replicator.thl.protobuf -- Generated protobuf
  code

  com.continuent.tungsten.replicator.thl.serializer -- Serialization
  implementation built on protobuf to translate between ReplDBMSEvent 
  classes and corresponding protobuf representation


HAZARDOUS DUTY WARNING

The THL is the most important feature of the replicator and must
meet the following requirements for replication to function correctly.

1.) Replicators store the full contents of all non-volatile transaction
data in the THL.

2.) Retrieved transactions are identical to stored transaction
value.

3.) Replicators operating with a new version of the THL can *always*
read an older THL.  Missing fields receive reasonable defaults.

For this reason protobuf definitions should not be changed lightly!!!
Be sure you understand exactly how protobuf works before making any
changes or you may corrupt the log. Here are rules to follow when
making changes:

1.) Never change format without a design review. 

2.) Never change the *numbering* of existing records and fields in
the protobuf definition. It is OK to change the *name* of fields
and records though this should never be done without good reason.

3.) Change the definition by addition only. 


SETTING UP

To make protobuf changes you will need the protoc tool in your path.
It reads the protobuf definition and generates Java code to handle
serialization details.

The protoc version must match the current protobuf library version.
E.g., if you are using protobuf-2.6.1.jar you must use the 2.6.1
protoc executable.

The simplest way to get a particular version of protoc is to build it.  Here
are the instructions for Mac OS X.  

1.) Install Xcode from Apple App Store. 

2.) Download and untar your selected protobuf version from
https://developers.google.com/protocol-buffers/docs/downloads.

3.) Follow the top-level README.txt instructions to build and install
protoc.

WARNING: protoc 2.4.1 does not build on Mac OS X. Later versions
build without problems.


MAKING CHANGES TO THE PROTOBUF FORMAT FOR THE THL

Follow the procedure shown below. 

1.) Add fields to the TungstenProtobufMessage file. 

2.) Regenerate the protobuf infrastructure code using 'ant protos'
in the replicator

3.) Add appropriate changes to class
com.continuent.tungsten.replicator.thl.serializer.ProtobufSerializer.
For example, if you add a new field to a protobuf record you should
ensure the values are written and read back from.


UPGRADING PROTOBUF VERSIONS

To upgrade protobuf library versions, do the following: 

1. Download the new protobuf library version. 

2. Build and install protoc in your path. 

3. Build the protobuf Java library, e.g., protobuf-java-2.6.1.jar.

IMPORTANT NOTE: Recent protobuf Jar files do not build on Mac OS
X. If you run into problems build on Linux and copy the file to Mac
OS X if you are working there.

4. Delete any previous protobuf Jar files from commons/lib and
replicator/lib.

5. Copy the new protobuf Jar file to commons/lib and replicator/lib.

6. Regenerate protobuf infrastructure code using 'ant protos' in
the replicator build.xml file.  

7. Test carefully!  Don't forget to test the ability to read
replicator logs from previous protobuf versions.
