## Data Richness
To describe data richness from different arrangements/organizations, "sparsity", "raw data without parsing", "additional provided information", and "diversity" are considered.  
In the following paragraph, we will focus on the observable metrics.

We prefer the analytic form (like `Data 2`) over the arbitrary format (like Data 1).
```
// for a simple exmaple

// Data 1
// FieldX: "mjjads_ValA_ValB_ValC_TTTT_oimwhdhal"

// Data 2
// FieldX1: "ValA"
// FieldX2: "ValB"
// FieldX3: "ValC"

// Data 3`
// FieldX1: "ValA"
// FieldX9: "ValB+ValC*10"
```

It conceptualizes  
`the oiginal degrees of freedom`,  
`non-empty expanded fields measured over a period of data`,  
and `the total levels/categories of fields`.

### Create Fake Data
* preprocess the subset of data for this analysis
* create a fake dataset from [create_fakedata.scala](src/tmp/create_fakedata.scala)
  ```
  # in spark shell for checking implementation
  scala> :load projects/DataRichness/src/tmp/create_fakedata.scala
  scala> make_fakedata()
  projects/DataRichness/data/Fake/2024/06/20/08H/Fake_2024_06_20_08H.parquet
  projects/DataRichness/data/Fake/2024/06/20/09H/Fake_2024_06_20_09H.parquet
  projects/DataRichness/data/Fake/2024/06/20/10H/Fake_2024_06_20_10H.parquet
  ```
* use a more deep structure like [realtime-bidding-proto](https://developers.google.com/authorized-buyers/rtb/downloads/realtime-bidding-proto)


### Degrees of Freedom
By recursively expanding nested structures into one-dimensional columns, we can compare data from different providers with the same physical meanings.
 This allows us to determine which data have more explained dimensions based on their structure declarations.
 However, this is not the actual degrees of freedom of the data; it's the declared degrees of freedom without considering the data itself.  
Others: The recursion in this topic is inefficient when running PySpark with large datasets.

The processing code [GenTable_schema1d.scala](src/main/scala/GenTable_schema1d.scala) generates a table that lists the one-dimensional columns and their data types from the prepared dataset.
```
# in spark shell
scala> :load projects/DataRichness/src/main/scala/GenTable_schema1d.scala
Loading projects/DataRichness/src/main/scala/GenTable_schema1d.scala...
...
defined object GenTable

scala> GenTable.main(Array("schema1d", "20240620_0800", "projects/DataRichness/table/Table_NonEmptyCols_fake.tsv", "3", "'projects/DataRichness/data/Fake/'yyyy/MM/dd/HH'H'/'Fake_'yyyy_MM_dd_HH'H.parquet'"))
...
timestamp BIGINT	timestamp	BIGINT	true
fake_data STRUCT<fake_id: BINARY>	fake_data.fake_id	BINARY	true
fake_data STRUCT<fake_field1: STRING>	fake_data.fake_field1	STRING	true
fake_data STRUCT<fake_field2_empty: INT>	fake_data.fake_field2_empty	INT	false
...
```
```
or run the defined task
projects/DataRichness/tasks/run_gentable_schema1d.sh
```

### Observable Degrees of Freedom
The non-empty expanded fields measured over a period of data are processed in [GenTable_schema1d.scala `def chk_emptycols`](src/main/scala/GenTable_schema1d.scala#L94).
 By aggregating over a period of data, fields are checked for `null` values, and the `explode` function in Spark is used to fully expand the fields.
 However, fields with only default values like 0 or an empty string are not yet excluded. This issue is addressed in a later paragraph. 

### Total Levels/Categories
The processing code [RecurCount_NonEmpty.scala](src/main/scala/RecurCount_NonEmpty.scala) calculates the non-empty total size for a selected one-dimensional column from the prepared dataset and table.
```
# in spark shell for checking implementation
scala> :load projects/DataRichness/src/tmp/chk_recurcount.scala
Loading projects/DataRichness/src/tmp/chk_recurcount.scala...
...
scala> df.show()
...
```
```
# or run the defined task
projects/DataRichness/tasks/run_recurcount_nonempty.sh
RESULT: fake_data.fake_field5.field5_br1[].field5_br1_2[], 	3000
```
```
# or submitting 1 partion by
projects/DataRichness/tasks/schedule_recurcount.sh 1
RESULT: fake_data.fake_field1, 	6000
RESULT: fake_data.fake_field3[], 	5143
RESULT: fake_data.fake_field5.field5_br1[].field5_br1_1, 	4500
RESULT: fake_data.fake_field5.field5_br1[].field5_br1_2[], 	3000
RESULT: fake_data.fake_field6, 	6000
```

For each one-dimensional column, we can estimate:
* `Density`: `Total Element Size` / `Number of Records`  
  shows the average element size between 0 (almost empty with rare information) and larger size (multiple values). It is already included in `RecurCount_NonEmpty.scala`.  
  For any other aggregating function, the implementation could be further modified within this framework.
* `Levels/Categories`: `Distinct Size`  
  shows how many levels/categories exist in the selected field. It is not included, but could be referenced by `GenTable_schema1d.scala def chk_emptycols` with `distinct()`.  
  For the question about whether the values are only default values like 0 or an empty string, they would be considered `1` from this estimation.

These estimates can be used to compare data from different providers with the same physical meaning, considering `estimated Degrees of Freedom`, `filled element Density`, and `total Levels/Categories`.
