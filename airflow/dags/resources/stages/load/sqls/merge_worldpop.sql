MERGE `{{ destination_dataset_project_id }}.{{ destination_dataset_name }}.{{ destination_table }}` T
USING `{{ source_dataset_project_id }}.{{ source_dataset_name }}.{{ source_table }}` S
ON FALSE
WHEN NOT MATCHED AND year={{year}} AND country='{{country}}' THEN
  INSERT (geography, geography_polygon, country, year, population) VALUES(geography, geography_polygon, country, year, population)
WHEN NOT MATCHED BY SOURCE AND year={{year}} AND country='{{country}}' THEN
  DELETE