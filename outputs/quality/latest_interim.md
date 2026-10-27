# Data quality — latest validation (interim layer, Spark engine)

Written by `python -m src.quality` (Great Expectations 1.24.1) from the files of the last pipeline run. Deterministic: no timestamps or run ids; those go to `outputs/quality/history/` (git-ignored). Rules come from [`metadata/catalog.yaml`](../../metadata/catalog.yaml); `DQ-n` marks an expectation that duplicates invariant n of `tests/test_data_quality.py`. A failing **gate** stops the pipeline; a **warning** is reported only.

**Result: PASS** — 70 expectations: 70 passed, 0 failed.

| Suite | Expectations | Passed | Gate failures | Warnings |
|---|---:|---:|---:|---:|
| interim | 70 | 70 | 0 | 0 |

## interim

| Check | What | Expectation | Kind | Result | Observed |
|---|---|---|---|---|---|
| `contacts_parquet.columns` | columns match the catalog, in order | `expect_table_columns_to_match_ordered_list` | gate | pass | 14 values |
| `contacts_parquet.rows` | rows in [3,500,000, 5,500,000] | `expect_table_row_count_to_be_between` | gate | pass | 4,740,671 |
| `contacts_parquet.primary_key` | primary key (contact_id) is unique | `expect_column_values_to_be_unique` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts_parquet.contact_id.not_null` | contact_id has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts_parquet.contact_id.pattern` | contact_id matches ^CT-[0-9]{7,}$ | `expect_column_values_to_match_regex` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts_parquet.timestamp.not_null` | timestamp has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts_parquet.date.not_null` | date has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts_parquet.interval_start.not_null` | interval_start has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts_parquet.hour.not_null` | hour has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts_parquet.hour.range` | hour in [0, 23] | `expect_column_values_to_be_between` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts_parquet.day_of_week.not_null` | day_of_week has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts_parquet.day_of_week.allowed` | day_of_week only takes allowed values | `expect_column_values_to_be_in_set` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts_parquet.contact_reason.not_null` | contact_reason has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts_parquet.contact_reason.allowed` | contact_reason only takes allowed values | `expect_column_values_to_be_in_set` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts_parquet.customer_segment.not_null` | customer_segment has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts_parquet.customer_segment.allowed` | customer_segment only takes allowed values | `expect_column_values_to_be_in_set` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts_parquet.handle_time_seconds.not_null` | handle_time_seconds has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts_parquet.handle_time_seconds.range` | handle_time_seconds in [30, 3,600] | `expect_column_values_to_be_between` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts_parquet.irop_flag.not_null` | irop_flag has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts_parquet.resolved_first_contact.not_null` | resolved_first_contact has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts_parquet.repeat_contact.not_null` | repeat_contact has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts_parquet.abandoned.not_null` | abandoned has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts_parquet.interval_index.not_null` | interval_index has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts_parquet.interval_index.range` | interval_index in [0, 47] | `expect_column_values_to_be_between` | gate | pass | 0 of 4,740,671 rows unexpected |
| `contacts_parquet.rows_equal_raw` | keeps every contacts.csv row (expected 4,740,671) | `expect_table_row_count_to_equal` | gate | pass | 4,740,671 |
| `spark_daily_series.columns` | columns match the catalog, in order | `expect_table_columns_to_match_ordered_list` | gate | pass | ds, y, irop_flag |
| `spark_daily_series.rows` | rows = 365 | `expect_table_row_count_to_equal` | gate | pass | 365 |
| `spark_daily_series.primary_key` | primary key (ds) is unique | `expect_column_values_to_be_unique` | gate | pass | 0 of 365 rows unexpected |
| `spark_daily_series.ds.not_null` | ds has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 365 rows unexpected |
| `spark_daily_series.y.not_null` | y has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 365 rows unexpected |
| `spark_daily_series.y.range` | y >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 365 rows unexpected |
| `spark_daily_series.irop_flag.not_null` | irop_flag has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 365 rows unexpected |
| `spark_daily_series.y.total_equals_raw` | daily totals sum to the raw rows (4,740,671) | `expect_column_sum_to_be_between` | gate | pass | 4,740,671 |
| `spark_intraday_profile.columns` | columns match the catalog, in order | `expect_table_columns_to_match_ordered_list` | gate | pass | interval_index, interval_start, fraction |
| `spark_intraday_profile.rows` | rows = 48 | `expect_table_row_count_to_equal` | gate | pass | 48 |
| `spark_intraday_profile.primary_key` | primary key (interval_index) is unique | `expect_column_values_to_be_unique` | gate | pass | 0 of 48 rows unexpected |
| `spark_intraday_profile.interval_index.not_null` | interval_index has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 48 rows unexpected |
| `spark_intraday_profile.interval_index.range` | interval_index in [0, 47] | `expect_column_values_to_be_between` | gate | pass | 0 of 48 rows unexpected |
| `spark_intraday_profile.interval_start.not_null` | interval_start has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 48 rows unexpected |
| `spark_intraday_profile.fraction.not_null` | fraction has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 48 rows unexpected |
| `spark_intraday_profile.fraction.range` | fraction in [0, 1] | `expect_column_values_to_be_between` | gate | pass | 0 of 48 rows unexpected |
| `spark_intraday_profile.fraction.sum_is_one` | fractions sum to 1 within 1e-9 | `expect_column_sum_to_be_between` | gate | pass | 1 |
| `spark_interval_aht.columns` | columns match the catalog, in order | `expect_table_columns_to_match_ordered_list` | gate | pass | interval_index, aht_seconds |
| `spark_interval_aht.rows` | rows = 48 | `expect_table_row_count_to_equal` | gate | pass | 48 |
| `spark_interval_aht.primary_key` | primary key (interval_index) is unique | `expect_column_values_to_be_unique` | gate | pass | 0 of 48 rows unexpected |
| `spark_interval_aht.interval_index.not_null` | interval_index has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 48 rows unexpected |
| `spark_interval_aht.interval_index.range` | interval_index in [0, 47] | `expect_column_values_to_be_between` | gate | pass | 0 of 48 rows unexpected |
| `spark_interval_aht.aht_seconds.not_null` | aht_seconds has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 48 rows unexpected |
| `spark_interval_aht.aht_seconds.range` | aht_seconds in [30, 3,600] | `expect_column_values_to_be_between` | gate | pass | 0 of 48 rows unexpected |
| `spark_reason_mix.columns` | columns match the catalog, in order | `expect_table_columns_to_match_ordered_list` | gate | pass | reason, contacts, avg_AHT, repeat_rate |
| `spark_reason_mix.rows` | rows = 7 | `expect_table_row_count_to_equal` | gate | pass | 7 |
| `spark_reason_mix.primary_key` | primary key (reason) is unique | `expect_column_values_to_be_unique` | gate | pass | 0 of 7 rows unexpected |
| `spark_reason_mix.reason.not_null` | reason has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 7 rows unexpected |
| `spark_reason_mix.reason.allowed` | reason only takes allowed values | `expect_column_values_to_be_in_set` | gate | pass | 0 of 7 rows unexpected |
| `spark_reason_mix.contacts.not_null` | contacts has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 7 rows unexpected |
| `spark_reason_mix.contacts.range` | contacts >= 0 | `expect_column_values_to_be_between` | gate | pass | 0 of 7 rows unexpected |
| `spark_reason_mix.avg_AHT.not_null` | avg_AHT has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 7 rows unexpected |
| `spark_reason_mix.avg_AHT.range` | avg_AHT in [30, 3,600] | `expect_column_values_to_be_between` | gate | pass | 0 of 7 rows unexpected |
| `spark_reason_mix.repeat_rate.not_null` | repeat_rate has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 7 rows unexpected |
| `spark_reason_mix.repeat_rate.range` | repeat_rate in [0, 1] | `expect_column_values_to_be_between` | gate | pass | 0 of 7 rows unexpected |
| `spark_reason_mix.contacts.total_equals_raw` | reason counts sum to the raw rows (4,740,671) | `expect_column_sum_to_be_between` | gate | pass | 4,740,671 |
| `spark_scalars.fields` | has exactly the catalog's fields | `expect_table_columns_to_match_set` | gate | pass | aht_irop, deflectable_share, n_contacts |
| `spark_scalars.n_contacts.not_null` | n_contacts has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `spark_scalars.n_contacts.range` | n_contacts in [3,500,000, 5,500,000] | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `spark_scalars.aht_irop.not_null` | aht_irop has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `spark_scalars.aht_irop.range` | aht_irop in [30, 3,600] | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `spark_scalars.deflectable_share.not_null` | deflectable_share has no nulls | `expect_column_values_to_not_be_null` | gate | pass | 0 of 1 rows unexpected |
| `spark_scalars.deflectable_share.range` | deflectable_share in [0, 1] | `expect_column_values_to_be_between` | gate | pass | 0 of 1 rows unexpected |
| `spark_scalars.n_contacts.equals_raw` | n_contacts equals the raw rows (expected 4,740,671) | `expect_column_max_to_be_between` | gate | pass | 4,740,671 |
| `interim.freshness` | every artifact is at least as new as its input | `expect_column_values_to_be_in_set` | gate | pass | 0 of 6 rows unexpected |
