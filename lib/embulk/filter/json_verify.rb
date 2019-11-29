module Embulk
  module Filter

    class JsonVerify < FilterPlugin
      Plugin.register_filter("json_verify", self)

      COMPAT_TABLE = {
        "Fixnum" => [ "INTEGER", "FLOAT" ],
        "String" => [ "STRING", "TIMESTAMP" ],
        "FLoat"  => [ "FLOAT" ],
        "TrueClass"  => [ "BOOLEAN" ],
        "FalseClass" => [ "BOOLEAN" ],
      }

      TIMESTAMP_FORMAT = /\A\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{1,4})? ((\+|-)\d{4}|UTC)\z/

      def self.transaction(config, in_schema, &control)
        task = {
          "schema_file" => config.param("schema_file", :string),
          "optional_fields" => config.param("optional_fields", :array, default: []),
          "json_column_name" => config.param("json_column_name", :string, default: "record"),
        }

        task["json_column"] = find_column(in_schema, task["json_column_name"])

        yield(task, in_schema)
      end

      def self.find_column(schema, column_name)
        index = schema.index { |field| field.name == column_name }
        type = schema[index].type

        # Even if we use symbol as key here, it will still converted to string.
        { "index" => index, "type" => type }
      end

      private_class_method :find_column

      def init
        # initialization code:
        @schema = JSON.parse(File.open(task["schema_file"]).read)
        @optional_fields = task["optional_fields"]
        @json_column = task["json_column"]
        @verified = false
      end

      def close
      end

      def add(page)
        page.each do |record|
          verify!(record)
          page_builder.add(record)
        end
      end

      def finish
        page_builder.finish
      end

      def verify!(record)
        return if @verified
        return unless preview?

        not_present = []
        invalid_types = []
        record = record[@json_column["index"]]

        case @json_column["type"]
        when "string"
          record = JSON.parse(record)
        when "json"
          # do nothing
        else
          raise ArgumentError.new("This filter can only work with json or string type")
        end

        record_keys = record.keys
        @schema.each do |field|

          # 1. Check whether this field present in the data record.
          if !record_keys.include?(field["name"])
            next if @optional_fields.include?(field["name"])
            not_present << field["name"]
            next
          end

          # 2. Check whether the type between schema and
          #    this data record is compatible.
          content = record[field["name"]]
          unless COMPAT_TABLE[content.class.name].include?(field["type"])
            invalid_types << {
              field: field["name"],
              data_type: content.class.name,
              schema_type: field["type"],
            }
            next
          end

          # 3. Check if string format is comply with TIMESTAMP
          if field["type"] == "TIMESTAMP" && content.is_a?(String)
            if content.match(TIMESTAMP_FORMAT).nil?
              invalid_types << {
                field: field["name"],
                data_type: content.class.name,
                schema_type: field["type"],
              }
            end
          end
        end

        if not_present.length > 0 || invalid_types.length > 0
          report(not_present, invalid_types)
        end

        @verified = true
      end

      def preview?
        @preview ||= begin
                       org.embulk.spi.Exec.isPreview()
                     rescue java.lang.NullPointerException
                       false
                     end
      end

      private

      def report(not_present, invalid_types)
        puts "---------------------------------"
        puts "required_columns that supposed to be present:\n#{not_present.inspect}"
        puts
        puts "columns with type incompatible with bigquery schema:\n#{invalid_types.inspect}"
        puts "---------------------------------"
      end
    end
  end
end
