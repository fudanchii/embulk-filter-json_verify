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

        # json schema
        "string" =>  [ "STRING", "TIMESTAMP" ],
        "integer" => [ "INTEGER", "FLOAT" ],
        "boolean" => [ "BOOLEAN" ],
        "double" =>  [ "FLOAT" ],
        "jsonobject" => [ "STRING" ], # likely overidden as string
        "jsonarray"  => [ "STRING" ], # likely overidden as string
      }

      TIMESTAMP_FORMAT = /\A\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{1,4})? ((\+|-)\d{4}|UTC)\z/

      def self.transaction(config, in_schema, &control)
        task = {
          "schema_file" => config.param("schema_file", :string),
          "optional_fields" => config.param("optional_fields", :array, default: []),
          "json_column_name" => config.param("json_column_name", :string, default: "record"),
          "json_schema_column" => config.param("json_schema_column", :string, default: "json_schema"),

          # [{
          #   name: "field_name at json_schema",
          #   use_name: "field_name to use when checking json data",
          #   use_type: "field_type to use when checking json data"
          # }, ... ]
          "override_fields" => config.param("override_fields", :array, default: [])
        }

        task["json_column"] = find_column(in_schema, task["json_column_name"])
        task["json_schema"] = find_column(in_schema, task["json_schema_column"])

        yield(task, in_schema)
      end

      def self.find_column(schema, column_name)
        index = schema.index { |field| field.name == column_name }

        if index.nil?
          raise(
            ArgumentError
              .new("no such column: #{column_name} in schema, " +
                   "we only have: #{schema.map { |field| field.name }.inspect}")
          )
        end

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
        @json_schema = task["json_schema"]
        @override_fields = task["override_fields"]
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

      def verify!(columns)
        return if @verified
        return unless preview?

        not_present = []
        invalid_types = []

        record = parse_json!(columns[@json_column["index"]], @json_column["type"])

        record_keys = record.keys

        # loop over the bigquery schema
        @schema.each do |field|

          # 1. Check whether this field present in the data record.
          if !record_keys.include?(field["name"])
            next if @optional_fields.include?(field["name"])
            not_present << field["name"]
            next
          end

          content = record[field["name"]]

          # 2. Check if current field's data is nil
          if content.nil?

            # only parse def at first record, then reuse for the the rest.
            @field_defs ||= begin
              fdef = parse_json!(columns[@json_schema["index"]], @json_schema["type"])
              override(fdef)
            end

            field_def = @field_defs.detect { |f| f["name"] == field["name"] }

            # append to invalid_types if defined type is not compatible with
            # bigquery schema.
            unless COMPAT_TABLE[field_def["type"]].include?(field["type"])
              invalid_types << {
                field: field["name"],
                data_type: field_def["type"],
                schema_type: field["type"],
              }
            end

          else

            # 3. Check whether the type between schema and
            #    this data record is compatible.
            unless COMPAT_TABLE[content.class.name].include?(field["type"])
              invalid_types << {
                field: field["name"],
                data_type: content.class.name,
                schema_type: field["type"],
              }
            end
          end # if content.nil?

          # 4. Check if string format is comply with TIMESTAMP
          if field["type"] == "TIMESTAMP" && content.is_a?(String)
            if content.match(TIMESTAMP_FORMAT).nil?
              invalid_types << {
                field: field["name"],
                data_type: content.class.name,
                schema_type: field["type"],
              }
            end
          end
        end # @schema.each

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

      def parse_json!(record, type)
        case type
        when "string"
          JSON.parse(record)
        when "json"
          record
        else
          raise ArgumentError.new("This filter can only work with json or string type")
        end
      end

      def report(not_present, invalid_types)
        STDERR.puts "---------------------------------"
        STDERR.puts "required_columns that supposed to be present:\n#{not_present.inspect}"
        STDERR.puts
        STDERR.puts "columns with type incompatible with bigquery schema:\n#{invalid_types.inspect}"
        STDERR.puts "---------------------------------"
      end

      # override schema definition returned from input plugin
      # to also consider added and typecasted columns
      # override format:
      # {
      #   name: "field_name", use_name: "name to be used", use_type: "type to be used"
      # }
      #
      # field_def format:
      # {
      #   name: "field_name", type: "field_type"
      # }
      #
      # possible field_type:
      # - string
      # - integer
      # - double
      # - boolean
      # - jsonobject
      # - jsonarray
      def override(field_def)
        field_def.tap do |fd|
          @override_fields.each do |ov|
            field = fd.detect { |f| f["name"] == ov["name"] }
            field["name"] = ov["use_name"] unless ov["use_name"].nil?
            field["type"] = ov["use_type"] unless ov["use_type"].nil?
          end
        end
      end
    end
  end
end
