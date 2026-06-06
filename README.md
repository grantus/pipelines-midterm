## Metrics

### 1. Number of card searches containing a target document

The metric filters `CardSearch` rows where `foundDocs` contains the configured target document id.

Default target document:

```text
ACC_45616
```

### 2. Openings of documents found through quick search by day

The metric:

1. selects quick searches;
2. joins document openings by `(sessionId, searchId)`;
3. keeps only openings where the opened document is present in the quick-search result list;
4. groups by `(day, documentId)`.

This attribution is intentionally implemented in the metric layer, not in the parser.

## Build

```bash
mvn clean test
mvn clean package
```

## Run

```bash
spark-submit \
  --class cpspark.Main \
  --master local[*] \
  target/cpspark-sessions-1.0.0.jar \
  --input /path/to/input \
  --output /path/to/output \
  --target-document ACC_45616 \
  --encoding windows-1251 \
  --zone Europe/Moscow
```

## Outputs

```text
<output>/card_search_count_for_ACC_45616
<output>/quick_search_openings_by_day
<output>/parse_rejects
```
