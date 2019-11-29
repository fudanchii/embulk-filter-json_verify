# Json Verify filter plugin for Embulk

Verify json data to comply with specified schema

## Overview

* **Plugin type**: filter

## Configuration

- **schema_file**: description (string, required)
- **optional_fields**: description (array, default: `[]`)
- **json_column_name**: description (string, default: `record`)

## Example

```yaml
filters:
  - type: json_verify
    schema_file: "schema/deals.json"
    optional_fields: [ "accreditation" ]
```
