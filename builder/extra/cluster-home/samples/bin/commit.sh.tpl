#!/bin/sh
PREPARE_DIR=@{PREPARE_DIRECTORY}
TARGET_DIR=@{TARGET_DIRECTORY}
if [ ! -d $PREPARE_DIR ]
then
	echo "$PREPARE_DIR is not present, it may have been promoted already."
	exit 1
fi
mv $PREPARE_DIR $TARGET_DIR
$TARGET_DIR/tools/tpm promote