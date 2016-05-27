module ActiveRecord
  module ConnectionAdapters
    class SQLServerColumn < Column

      def initialize(name, default, cast_type, sql_type = nil, null = true, sqlserver_options = {})
        super(name, default, cast_type, sql_type, null)
        @sqlserver_options = sqlserver_options.symbolize_keys
        @default_function = @sqlserver_options[:default_function]
        @primary = @sqlserver_options[:is_identity] || @sqlserver_options[:is_primary]
      end

      class << self

        def string_to_binary(value)
          "0x#{value.unpack("H*")[0]}"
        end

        def binary_to_string(value)
          value =~ /[^[:xdigit:]]/ ? value : [value].pack('H*')
        end

      end

      def sql_type_for_statement
        if is_integer? || is_real?
          sql_type.sub(/\((\d+)?\)/, '')
        else
          sql_type
        end
      end

      def is_identity?
        @sqlserver_options[:is_identity]
      end

      def is_primary?
        @sqlserver_options[:is_primary]
      end

      def default_function
        @sqlserver_options[:default_function]
      end

      def table_name
        @sqlserver_options[:table_name]
      end

      def is_utf8?
        !!(@sql_type =~ /nvarchar|ntext|nchar/i)
      end

      def is_integer?
        !!(@sql_type =~ /int/i)
      end

      def table_klass
        @table_klass ||= begin
          table_name.classify.constantize
        rescue StandardError, NameError, LoadError
          nil
        end
        (@table_klass && @table_klass < ActiveRecord::Base) ? @table_klass : nil
      end

      def is_real?
        !!(@sql_type =~ /real/i)
      end

      def database_year
        @sqlserver_options[:database_year]
      end

      def collation
        @sqlserver_options[:collation]
      end

      def case_sensitive?
        collation && !collation.match(/_CI/)
      end

      private

      def extract_limit(sql_type)
        case sql_type
          when /^smallint/i
            2
          when /^int/i
            4
          when /^bigint/i
            8
          when /\(max\)/, /decimal/, /numeric/
            nil
          else
            super
        end
      end

      def simplified_type(field_type)
        case field_type
          when /real/i              then :float
          when /money/i             then :decimal
          when /image/i             then :binary
          when /bit/i               then :boolean
          when /uniqueidentifier/i  then :string
          when /datetime/i          then simplified_datetime
          when /varchar\(max\)/     then :text
          when /timestamp/          then :binary
          else super
        end
      end

      def simplified_datetime
        if database_year >= 2008
          :datetime
        elsif table_klass && table_klass.coerced_sqlserver_date_columns.include?(name)
          :date
        elsif table_klass && table_klass.coerced_sqlserver_time_columns.include?(name)
          :time
        else
          :datetime
        end
      end

    end #class SQLServerColumn
  end #module ConnectionAdapters
end #module ActiveRecord