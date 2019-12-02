# Json Verify filter plugin for Embulk

Verify json data to comply with specified schema

## Overview

* **Plugin type**: filter

## Configuration

- **schema_file**: schema file path to bigquery schema (string, required)
- **optional_fields**: list of fields that is ok to be not present (array, default: `[]`)
- **json_column_name**: the name of the column where json data record located (string, default: `record`)
- **json_schema_column**: the name of the column where json schema located (string, default: `json_schema`)
- **override**: value to override in the json_schema (string, default: `json_schema`)

## Example

```yaml
filters:
  - type: json_verify
    schema_file: "schema/deals.json"
    optional_fields: [ "accreditation" ]
    override:
      - { name: "id", use_name: "deals_id" }
      - { name: "owner", use_type: "string" }
```
