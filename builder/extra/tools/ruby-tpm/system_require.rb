#
# TUNGSTEN SCALE-OUT STACK
# Copyright (C) 2009 Continuent, Inc.
# All rights reserved
#

# Creates method to load system libraries with a nice error message 
# when there is a failure. 

# Override Kernel.require. 
def system_require(lib)
  require_cmd = "require '#{lib}'"
  begin
    eval require_cmd
  rescue LoadError
    $stderr.printf "=============================================================\n"
    $stderr.printf "ERROR:  Unable to load required Ruby module: #{lib}\n"
    $stderr.print "Please ensure Ruby system modules are properly installed\n"
    $stderr.printf "=============================================================\n"
    $stderr.printf "\nFull error and stack trace follows...\n"
    raise
  end
end
