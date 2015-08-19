require 'json/common'
module JSON
  require 'json/version'

  # We only want to use the pure JSON module
  #begin
  #  require 'json/ext'
  #rescue LoadError
    require 'json/pure'
  #end
end
