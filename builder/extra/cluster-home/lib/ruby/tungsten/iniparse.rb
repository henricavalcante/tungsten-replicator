require 'iniparse'

module IniParse
  module Lines
    class BlankOption
      include Line

      @regex = /^\s*([^=]+)  # Option
                 $
               /x

      attr_accessor :key, :value

      # ==== Parameters
      # key<String>::   The option key.
      # value<String>:: The value for this option.
      # opts<Hash>::    Extra options for the line.
      #
      def initialize(key, value = nil, opts = {})
        super(opts)
        @key, @value = nil, value
      end

      def self.parse(line, opts)
        if m = @regex.match(line)
          [:option, m[1].strip, nil, opts]
        end
      end

      #######
      private
      #######

      def line_contents
        '%s' % [key]
      end
    end
  end
end

module IniParse
  module Lines
    class Option
      def self.typecast(value)
        value
      end
    end
  end
end

IniParse::Parser.parse_types = [IniParse::Lines::Section,
  IniParse::Lines::Option, IniParse::Lines::BlankOption, 
  IniParse::Lines::Blank]