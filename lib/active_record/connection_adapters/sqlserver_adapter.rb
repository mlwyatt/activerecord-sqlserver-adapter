require 'base64'
require 'arel_sqlserver'
require 'arel/visitors/sqlserver'
require 'active_record'
require 'active_record/base'
require 'active_support/concern'
require 'active_record/sqlserver_base'
require 'active_support/core_ext/string'
require 'active_record/tasks/sqlserver_database_tasks'
require 'active_record/connection_adapters/sqlserver/type'
require 'active_record/connection_adapters/sqlserver/utils'
require 'active_record/connection_adapters/abstract_adapter'
require 'active_record/connection_adapters/sqlserver/errors'
require 'active_record/connection_adapters/sqlserver_column'
require 'active_record/connection_adapters/sqlserver/version'
require 'active_record/connection_adapters/sqlserver/quoting'
require 'active_record/connection_adapters/sqlserver/showplan'
require 'active_record/connection_adapters/sqlserver/transaction'
require 'active_record/connection_adapters/sqlserver/schema_cache'
require 'active_record/connection_adapters/sqlserver/database_tasks'
require 'active_record/connection_adapters/sqlserver/database_limits'
require 'active_record/connection_adapters/sqlserver/schema_creation'
require 'active_record/connection_adapters/sqlserver/table_definition'
require 'active_record/connection_adapters/sqlserver/core_ext/explain'
require 'active_record/connection_adapters/sqlserver/core_ext/relation'
require 'active_record/connection_adapters/sqlserver/schema_statements'
require 'active_record/connection_adapters/sqlserver/database_statements'
require 'active_record/connection_adapters/sqlserver/core_ext/active_record'
require 'active_record/connection_adapters/sqlserver/core_ext/attribute_methods'
require 'active_record/connection_adapters/sqlserver/core_ext/explain_subscriber'
require 'active_record/connection_adapters/sqlserver/core_ext/database_statements'

module ActiveRecord

  class Base

    def self.sqlserver_connection(config) #:nodoc:
      config = config.symbolize_keys
      config.reverse_merge! :mode => :dblib
      mode = config[:mode].to_s.downcase.underscore.to_sym
      case mode
        when :dblib
          require 'tiny_tds'
        when :odbc
          raise ArgumentError, 'Missing :dsn configuration.' unless config.has_key?(:dsn)
          require 'odbc'
          require 'active_record/connection_adapters/sqlserver/core_ext/odbc'
        else
          raise ArgumentError, "Unknown connection mode in #{config.inspect}."
      end
      ConnectionAdapters::SQLServerAdapter.new(nil, logger, nil, config.merge(:mode=>mode))
    end

    protected

    def self.did_retry_sqlserver_connection(connection,count)
      logger.info "CONNECTION RETRY: #{connection.class.name} retry ##{count}."
    end

    def self.did_lose_sqlserver_connection(connection)
      logger.info "CONNECTION LOST: #{connection.class.name}"
    end

  end

  module Tasks
    module DatabaseTasks
      register_task(/sqlserver/,ActiveRecord::Tasks::SQLServerDatabaseTasks)
    end
  end

  module ConnectionAdapters

    class SQLServerColumn < Column

      def initialize(name, default, sql_type = nil, null = true, sqlserver_options = {})
        @sqlserver_options = sqlserver_options.symbolize_keys
        super(name, default, sql_type, null)
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

      def is_identity?
        @sqlserver_options[:is_identity]
      end

      def is_primary?
        @sqlserver_options[:is_primary]
      end

      def is_utf8?
        !!(@sql_type =~ /nvarchar|ntext|nchar/i)
      end

      def is_integer?
        !!(@sql_type =~ /int/i)
      end

      def is_real?
        !!(@sql_type =~ /real/i)
      end

      def sql_type_for_statement
        if is_integer? || is_real?
          sql_type.sub(/\((\d+)?\)/,'')
        else
          sql_type
        end
      end

      def default_function
        @sqlserver_options[:default_function]
      end

      def table_name
        @sqlserver_options[:table_name]
      end

      def table_klass
        @table_klass ||= begin
          table_name.classify.constantize
        rescue StandardError, NameError, LoadError
          nil
        end
        (@table_klass && @table_klass < ActiveRecord::Base) ? @table_klass : nil
      end

      def database_year
        @sqlserver_options[:database_year]
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

    class SQLServerAdapter < AbstractAdapter

      include SQLServer::Version
      include SQLServer::Quoting
      include SQLServer::DatabaseStatements
      include SQLServer::Showplan
      include SQLServer::SchemaStatements
      include SQLServer::DatabaseLimits
      include SQLServer::DatabaseTasks
      include Sqlserver::Quoting
      include Sqlserver::DatabaseStatements
      include Sqlserver::Showplan
      include Sqlserver::SchemaStatements
      include Sqlserver::DatabaseLimits
      include Sqlserver::Errors

      VERSION                     = File.read(File.expand_path("../../../../VERSION",__FILE__)).strip
      ADAPTER_NAME                = 'SQLServer'.freeze
      DATABASE_VERSION_REGEXP     = /Microsoft SQL Server\s+"?(\d{4}|\w+)"?/
      SUPPORTED_VERSIONS          = [2000]
      ADAPTER_NAME = 'SQLServer'.freeze

      attr_reader :database_version, :database_year, :spid, :product_level, :product_version, :edition

      cattr_accessor :native_text_database_type, :native_binary_database_type, :native_string_database_type,
                     :enable_default_unicode_types, :auto_connect, :retry_deadlock_victim,
                     :cs_equality_operator, :lowercase_schema_reflection, :auto_connect_duration,
                     :showplan_option

      self.enable_default_unicode_types = true
      cattr_accessor :cs_equality_operator, instance_accessor: false
      cattr_accessor :use_output_inserted, instance_accessor: false
      cattr_accessor :lowercase_schema_reflection, :showplan_option

      self.cs_equality_operator = 'COLLATE Latin1_General_CS_AS_WS'
      self.use_output_inserted = true

      def initialize(connection, logger, pool, config)
        super(connection, logger, pool)
        # AbstractAdapter Responsibility
        @schema_cache = SQLServer::SchemaCache.new self
        @visitor = Arel::Visitors::SQLServer.new self
        #@prepared_statements = true
        # Our Responsibility
        @config = config
        @connection_options = config
        connect
        @database_version = select_value 'SELECT @@version', 'SCHEMA'
        @database_year = begin
          if @database_version =~ /Microsoft SQL Azure/i
            @sqlserver_azure = true
            @database_version.match(/\s(\d{4})\s/)[1].to_i
          else
            year = DATABASE_VERSION_REGEXP.match(@database_version)[1]
            year == "Denali" ? 2011 : year.to_i
          end
        rescue
          0
        end
        @product_level    = select_value "SELECT CAST(SERVERPROPERTY('productlevel') AS VARCHAR(128))", 'SCHEMA'
        @product_version  = select_value "SELECT CAST(SERVERPROPERTY('productversion') AS VARCHAR(128))", 'SCHEMA'
        @edition          = select_value "SELECT CAST(SERVERPROPERTY('edition') AS VARCHAR(128))", 'SCHEMA'
        @sqlserver_azure = !!(select_value('SELECT @@version', 'SCHEMA') =~ /Azure/i)
        initialize_dateformatter
        use_database
        unless SUPPORTED_VERSIONS.include?(@database_year)
          raise NotImplementedError, "Currently, only #{SUPPORTED_VERSIONS.to_sentence} are supported. We got back #{@database_version}."
        end
      end

      # === Abstract Adapter ========================================== #

      def valid_type?(type)
        !native_database_types[type].nil?
      end

      def schema_creation
        SQLServer::SchemaCreation.new self
      end

      def adapter_name
        ADAPTER_NAME
      end

      def supports_migrations?
        true
      end

      def supports_primary_key?
        true
      end

      def supports_count_distinct?
        true
      end

      def supports_ddl_transactions?
        true
      end

      def supports_bulk_alter?
        false
      end

      def supports_index_sort_order?
        true
      end

      def supports_savepoints?
        true
      end

      def supports_partial_index?
        true
      end

      def supports_explain?
        true
      end

      def supports_transaction_isolation?
        true
      end

      def supports_views?
        true
      end

      def supports_foreign_keys?
        true
      end

      def disable_referential_integrity
        tables = tables_with_referential_integrity
        tables.each { |t| do_execute "ALTER TABLE #{t} NOCHECK CONSTRAINT ALL" }
        do_execute "EXEC sp_MSforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL'"
        yield
      ensure
        tables.each { |t| do_execute "ALTER TABLE #{t} CHECK CONSTRAINT ALL" }
        do_execute "EXEC sp_MSforeachtable 'ALTER TABLE ? CHECK CONSTRAINT ALL'"
      end

      # === Abstract Adapter (Connection Management) ================== #

      def active?
        return false unless @connection
        raw_connection_do 'SELECT 1'
        true
        case @connection_options[:mode]
          when :dblib
            return @connection.active?
        end
        raw_connection_do("SELECT 1")
        true
      rescue *lost_connection_exceptions
        false
      end

      def reconnect!
        super
        disconnect!
        connect
        active?
      end

      def disconnect!
        super
        @spid = nil
        case @connection_options[:mode]
          when :dblib
            @connection.close rescue nil
          when :odbc
            @connection.disconnect rescue nil
        end
        @connection = nil
      end

      def reset!
        reset_transaction
        do_execute 'IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION'
        remove_database_connections_and_rollback { }
      end

      # === Abstract Adapter (Misc Support) =========================== #

      def tables_with_referential_integrity
        schemas_and_tables = select_rows <<-SQL.strip_heredoc
          SELECT s.name, o.name
          FROM sys.foreign_keys i
          INNER JOIN sys.objects o ON i.parent_object_id = o.OBJECT_ID
          INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
        SQL
        schemas_and_tables.map do |schema_table|
          schema, table = schema_table
          "#{SQLServer::Utils.quoted_raw(schema)}.#{SQLServer::Utils.quoted_raw(table)}"
        end
      end

      def pk_and_sequence_for(table_name)
        pk = primary_key(table_name)
        pk ? [pk, nil] : nil
        idcol = identity_column(table_name)
        idcol ? [idcol.name,nil] : nil
      end

      def primary_key(table_name)
        schema_cache.columns(table_name).find(&:is_primary?).try(:name) || identity_column(table_name).try(:name)
        identity_column(table_name).try(:name) || schema_cache.columns[table_name].detect(&:is_primary?).try(:name)
      end

      # === SQLServer Specific (DB Reflection) ======================== #

      def sqlserver?
        true
      end

      def sqlserver_2000?
        @database_year == 2000
      end

      def database_prefix_remote_server?
        return false if database_prefix.blank?
        name = SQLServer::Utils.extract_identifiers(database_prefix)
        name.fully_qualified? && name.object.blank?
      end

      def database_prefix
        @connection_options[:database_prefix]
      end

      def version
        self.class::VERSION
      end

      def inspect
        "#<#{self.class} version: #{version}, mode: #{@connection_options[:mode]}, azure: #{sqlserver_azure?.inspect}>"
        "#<#{self.class} version: #{version}, year: #{@database_year}, product_level: #{@product_level.inspect}, product_version: #{@product_version.inspect}, edition: #{@edition.inspect}, connection_options: #{@connection_options.inspect}>"
      end

      def auto_connect
        @@auto_connect.is_a?(FalseClass) ? false : true
      end

      def auto_connect_duration
        @@auto_connect_duration ||= 10
      end

      def retry_deadlock_victim
        @@retry_deadlock_victim.is_a?(FalseClass) ? false : true
      end
      alias :retry_deadlock_victim? :retry_deadlock_victim

      def native_string_database_type
        @@native_string_database_type || (enable_default_unicode_types ? 'nvarchar' : 'varchar')
      end

      def native_text_database_type
        @@native_text_database_type || enable_default_unicode_types ? 'nvarchar(max)' : 'varchar(max)'
      end

      def native_time_database_type
        sqlserver_2005? ? 'datetime' : 'time'
      end

      def native_date_database_type
        sqlserver_2005? ? 'datetime' : 'date'
      end

      def native_binary_database_type
        @@native_binary_database_type || 'varbinary(max)'
      end

      def cs_equality_operator
        @@cs_equality_operator || 'COLLATE Latin1_General_CS_AS_WS'
      end

      protected

      # === Abstract Adapter (Misc Support) =========================== #

      def initialize_type_map(m)
        m.register_type              %r{.*},            SQLServer::Type::UnicodeString.new
        # Exact Numerics
        register_class_with_limit m, 'bigint(8)',         SQLServer::Type::BigInteger
        m.alias_type                 'bigint',            'bigint(8)'
        register_class_with_limit m, 'int(4)',            SQLServer::Type::Integer
        m.alias_type                 'integer',           'int(4)'
        m.alias_type                 'int',               'int(4)'
        register_class_with_limit m, 'smallint(2)',       SQLServer::Type::SmallInteger
        m.alias_type                 'smallint',          'smallint(2)'
        register_class_with_limit m, 'tinyint(1)',        SQLServer::Type::TinyInteger
        m.alias_type                 'tinyint',           'tinyint(1)'
        m.register_type              'bit',               SQLServer::Type::Boolean.new
        m.register_type              %r{\Adecimal}i do |sql_type|
          scale = extract_scale(sql_type)
          precision = extract_precision(sql_type)
          SQLServer::Type::Decimal.new precision: precision, scale: scale
        end
        m.alias_type                 %r{\Anumeric}i,      'decimal'
        m.register_type              'money',             SQLServer::Type::Money.new
        m.register_type              'smallmoney',        SQLServer::Type::SmallMoney.new
        # Approximate Numerics
        m.register_type              'float',             SQLServer::Type::Float.new
        m.register_type              'real',              SQLServer::Type::Real.new
        # Date and Time
        m.register_type              'date',              SQLServer::Type::Date.new
        m.register_type              'datetime',          SQLServer::Type::DateTime.new
        m.register_type              %r{\Adatetime2}i do |sql_type|
          precision = extract_precision(sql_type)
          SQLServer::Type::DateTime2.new precision: precision
        end
        m.register_type              %r{\Adatetimeoffset}i do |sql_type|
          precision = extract_precision(sql_type)
          SQLServer::Type::DateTimeOffset.new precision: precision
        end
        m.register_type              'smalldatetime',     SQLServer::Type::SmallDateTime.new
        m.register_type              %r{\Atime}i do |sql_type|
          scale = extract_scale(sql_type)
          precision = extract_precision(sql_type)
          SQLServer::Type::Time.new precision: precision
        end
        # Character Strings
        register_class_with_limit m, %r{\Achar}i,         SQLServer::Type::Char
        register_class_with_limit m, %r{\Avarchar}i,      SQLServer::Type::Varchar
        m.register_type              'varchar(max)',      SQLServer::Type::VarcharMax.new
        m.register_type              'text',              SQLServer::Type::Text.new
        # Unicode Character Strings
        register_class_with_limit m, %r{\Anchar}i,        SQLServer::Type::UnicodeChar
        register_class_with_limit m, %r{\Anvarchar}i,     SQLServer::Type::UnicodeVarchar
        m.alias_type                 'string',            'nvarchar(4000)'
        m.register_type              'nvarchar(max)',     SQLServer::Type::UnicodeVarcharMax.new
        m.register_type              'ntext',             SQLServer::Type::UnicodeText.new
        # Binary Strings
        register_class_with_limit m, %r{\Abinary}i,       SQLServer::Type::Binary
        register_class_with_limit m, %r{\Avarbinary}i,    SQLServer::Type::Varbinary
        m.register_type              'varbinary(max)',    SQLServer::Type::VarbinaryMax.new
        # Other Data Types
        m.register_type              'uniqueidentifier',  SQLServer::Type::Uuid.new
        m.register_type              'timestamp',         SQLServer::Type::Timestamp.new
      end

      def translate_exception(e, message)
        case message
          when /(cannot insert duplicate key .* with unique index) | (violation of unique key constraint)/i
            RecordNotUnique.new(message, e)
          when /cannot insert duplicate key .* with unique index/i
            RecordNotUnique.new(message,e)
          when /conflicted with the foreign key constraint/i
            InvalidForeignKey.new(message, e)
          when /has been chosen as the deadlock victim/i
            DeadlockVictim.new(message, e)
          when /database .* does not exist/i
            NoDatabaseError.new(message, e)
          when *lost_connection_messages
            LostConnection.new(message,e)
          else
            super
        end
      end

      # === SQLServer Specific (Connection Management) ================ #

      def connect
        config = @connection_options
        @connection = case config[:mode]
                        when :dblib
                          appname = config[:appname] || configure_application_name || Rails.application.class.name.split('::').first rescue nil
                          login_timeout = config[:login_timeout].present? ? config[:login_timeout].to_i : nil
                          timeout = config[:timeout].present? ? config[:timeout].to_i/1000 : nil
                          encoding = config[:encoding].present? ? config[:encoding] : nil
                          TinyTds::Client.new({
                                                  :dataserver    => config[:dataserver],
                                                  :host          => config[:host],
                                                  :port          => config[:port],
                                                  :username      => config[:username],
                                                  :password      => config[:password],
                                                  :database      => config[:database],
                                                  :appname       => appname,
                                                  :login_timeout => login_timeout,
                                                  :timeout       => timeout,
                                                  :encoding      => encoding,
                                                  :azure         => config[:azure]
                                              }).tap do |client|
                            if config[:azure]
                              client.execute("SET ANSI_NULLS ON").do
                              client.execute("SET CURSOR_CLOSE_ON_COMMIT OFF").do
                              client.execute("SET ANSI_NULL_DFLT_ON ON").do
                              client.execute("SET IMPLICIT_TRANSACTIONS OFF").do
                              client.execute("SET ANSI_PADDING ON").do
                              client.execute("SET QUOTED_IDENTIFIER ON")
                              client.execute("SET ANSI_WARNINGS ON").do
                            else
                              client.execute("SET ANSI_DEFAULTS ON").do
                              client.execute("SET CURSOR_CLOSE_ON_COMMIT OFF").do
                              client.execute("SET IMPLICIT_TRANSACTIONS OFF").do
                            end
                            client.execute("SET TEXTSIZE 2147483647").do
                          end
                        when :odbc
                          if config[:dsn].include?(';')
                            driver = ODBC::Driver.new.tap do |d|
                              d.name = config[:dsn_name] || 'Driver1'
                              d.attrs = config[:dsn].split(';').map{ |atr| atr.split('=') }.reject{ |kv| kv.size != 2 }.inject({}){ |h,kv| k,v = kv ; h[k] = v ; h }
                            end
                            ODBC::Database.new.drvconnect(driver)
                          else
                            ODBC.connect config[:dsn], config[:username], config[:password]
                          end.tap do |c|
                            begin
                              c.use_time = true
                              c.use_utc = ActiveRecord::Base.default_timezone == :utc
                            rescue Exception => e
                              warn "Ruby ODBC v0.99992 or higher is required."
                            end
                          end
                      end
        puts @connection
        @spid = _raw_select('SELECT @@SPID', fetch: :rows).first.first
        configure_connection
      rescue
        raise unless @auto_connecting
      end

      # Override this method so every connection can be configured to your needs.
      # For example:
      #    raw_connection_do "SET TEXTSIZE #{64.megabytes}"
      #    raw_connection_do "SET CONCAT_NULL_YIELDS_NULL ON"
      def configure_connection
      end

      # Override this method so every connection can have a unique name. Max 30 characters. Used by TinyTDS only.
      # For example:
      #    "myapp_#{$$}_#{Thread.current.object_id}".to(29)
      def configure_application_name
      end

      def connection_errors
        @connection_errors ||= [].tap do |errors|
          errors << TinyTds::Error if defined?(TinyTds::Error)
          errors << ODBC::Error if defined?(ODBC::Error)
        end
      end

      def config_appname(config)
        config[:appname] || configure_application_name || Rails.application.class.name.split('::').first rescue nil
      end

      def config_login_timeout(config)
        config[:login_timeout].present? ? config[:login_timeout].to_i : nil
      end

      def config_timeout(config)
        config[:timeout].present? ? config[:timeout].to_i / 1000 : nil
      end

      def config_encoding(config)
        config[:encoding].present? ? config[:encoding] : nil
      end

      def configure_connection ; end

      def configure_application_name ; end

      def initialize_dateformatter
        @database_dateformat = user_options_dateformat
        a, b, c = @database_dateformat.each_char.to_a
        [a, b, c].each { |f| f.upcase! if f == 'y' }
        dateformat = "%#{a}-%#{b}-%#{c}"
        ::Date::DATE_FORMATS[:_sqlserver_dateformat]     = dateformat
        ::Time::DATE_FORMATS[:_sqlserver_dateformat]     = dateformat
        ::Time::DATE_FORMATS[:_sqlserver_time]           = '%H:%M:%S'
        ::Time::DATE_FORMATS[:_sqlserver_datetime]       = "#{dateformat} %H:%M:%S"
        ::Time::DATE_FORMATS[:_sqlserver_datetimeoffset] = lambda { |time|
          time.strftime "#{dateformat} %H:%M:%S.%9N #{time.formatted_offset}"
        }
      end

      def remove_database_connections_and_rollback(database=nil)
        database ||= current_database
        do_execute "ALTER DATABASE #{quote_table_name(database)} SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
        begin
          yield
        ensure
          do_execute "ALTER DATABASE #{quote_table_name(database)} SET MULTI_USER"
        end if block_given?
      end

      def with_sqlserver_error_handling
        begin
          yield
        rescue Exception => e
          case translate_exception(e,e.message)
            when LostConnection; retry if auto_reconnected?
            when DeadlockVictim; retry if retry_deadlock_victim? && open_transactions == 0
          end
          raise
        end
      end

      def disable_auto_reconnect
        old_auto_connect, self.class.auto_connect = self.class.auto_connect, false
        yield
      ensure
        self.class.auto_connect = old_auto_connect
      end

      def auto_reconnected?
        return false unless auto_connect
        @auto_connecting = true
        count = 0
        while count <= (auto_connect_duration / 2)
          sleep 2** count
          ActiveRecord::Base.did_retry_sqlserver_connection(self,count)
          return true if reconnect!
          count += 1
        end
        ActiveRecord::Base.did_lose_sqlserver_connection(self)
        false
      ensure
        @auto_connecting = false
      end

    end #class SQLServerAdapter < AbstractAdapter

  end #module ConnectionAdapters

end #module ActiveRecord
