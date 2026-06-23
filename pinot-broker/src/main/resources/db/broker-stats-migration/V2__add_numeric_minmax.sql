--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
--

-- Numeric shadow columns for min/max, populated only for columns whose stored type is numeric and
-- whose min_value/max_value parse as a number. They let the segment-aware selectivity estimator
-- push the per-segment numeric range prune into SQL (string columns are pruned in Java instead).
-- NULL for non-numeric or unparseable values.
ALTER TABLE segment_col_stats ADD COLUMN min_num REAL;
ALTER TABLE segment_col_stats ADD COLUMN max_num REAL;

-- Stable 64-bit id of the segment name (FNV-1a, computed by the broker at upsert). The segment-aware
-- selectivity prune reads this INTEGER instead of the segment_name TEXT, so the planning read path
-- composes surviving-segment sets over cheap primitive longs and never materializes the name string.
ALTER TABLE segment_col_stats ADD COLUMN segment_id INTEGER;

-- The leading (table_name, column_name) prefix serves the per-column lookup for both the numeric
-- prune and the string all-rows fetch. SQLite can seek on the first inequality (min_num <= ?);
-- the second bound (max_num >= ?) is applied as a residual filter on the seeked rows. At the
-- hundreds-of-segments-per-table reality this is ample.
CREATE INDEX idx_segment_col_stats_num
  ON segment_col_stats(table_name, column_name, min_num, max_num);
