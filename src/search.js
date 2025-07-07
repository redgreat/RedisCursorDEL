// 创建索引5
DELETE /test_index

PUT /test_index
{ "settings": {
      "index.default_pipeline": "add_timestamp"
    },
  "mappings": {
      "properties": {
        "createTime": {
          "type": "date",
          "format": "yyyy-MM-dd HH:mm:ss",
          "ignore_malformed": false
        },
        "mysqlInsertTime": {
         "type": "date",
          "format": "yyyy-MM-dd HH:mm:ss",
          "ignore_malformed": false
        },
        "receiveTime": {
         "type": "date",
          "format": "yyyy-MM-dd HH:mm:ss",
          "ignore_malformed": false
        },
        "created_at": {
         "type": "date",
          "index": true,
          "store": true
        },
        "name": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "Name_1": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "Name_2": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "Name_3": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "Name_4": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "Name_5": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "Name_6": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "Name_7": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "Name_8": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "Name_9": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "id": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "@timestamp": {
          "type": "date",
          "format": "strict_date_optional_time||epoch_millis"
        },
        "consumeTime": {
         "type": "date",
          "format": "yyyy-MM-dd HH:mm:ss",
          "ignore_malformed": false
        }
      }
  }
}

PUT _ingest/pipeline/add_timestamp
{
  "description": "Automatically adds a @timestamp field with the ingestion time",
  "processors": [
    {
      "set": {
        "field": "@timestamp",
        "value": "{{_ingest.timestamp}}",
        "override": true
      }
    }
  ]
}

GET /test_index/_search
{
  "query": {
    "bool": {
      "must": [
        {
          "exists": {
            "field": "createTime"
          }
        },
        {
          "exists": {
            "field": "mysqlInsertTime"
          }
        }
      ]
    }
  },
  "size": 10000,
  "sort": [
    {
      "_script": {
        "type": "number",
        "script": {
          "lang": "painless",
          "source": """
            long createTimeMillis = doc['@timestamp'].value.toInstant().toEpochMilli();
            ZonedDateTime mysqlInsertTimeZoned = ZonedDateTime.ofInstant(doc['mysqlInsertTime'].value.toInstant(), ZoneId.of('UTC'));
            long mysqlInsertTimeMillis = mysqlInsertTimeZoned.toInstant().toEpochMilli();
            return createTimeMillis - mysqlInsertTimeMillis;
          """
        },
        "order": "desc"
      }
    }
  ]
}


GET /test_index/_search
{
  "size": 0,
  "aggs": {
    "time_diff": {
      "scripted_metric": {
        "init_script": "state.diffs = []",
        "map_script": """
          if (doc['consumeTimep'].size() > 0 && doc['mysqlInsertTime'].size() > 0) {
            state.diffs.add(doc['consumeTime'].value.millis - doc['mysqlInsertTime'].value.millis);
          }
        """,
        "combine_script": """
          if (state.diffs.size() == 0) {
            return null;
          }
          return state.diffs.stream().mapToLong(Long::longValue).average().orElse(0);
        """,
        "reduce_script": """
          if (states.size() == 0) {
            return null;
          }
          double total = 0;
          int count = 0;
          for (state in states) {
            if (state != null) {
              total += state;
              count++;
            }
          }
          return count == 0 ? null : total / count / 1000;
        """
      }
    }
  }
}

GET /test_index/_search
{
  "size": 0,
  "aggs": {
    "time_diff": {
      "scripted_metric": {
        "init_script": "state.diffs = []",
        "map_script": """
          if (doc['@timestamp'].size() > 0 && doc['mysqlInsertTime'].size() > 0) {
            long timestampMillis = doc['@timestamp'].value.millis;
            
            ZonedDateTime mysqlInsertTimeZoned = ZonedDateTime.ofInstant(doc['mysqlInsertTime'].value.toInstant(), ZoneId.of('Asia/Shanghai'));
            long mysqlInsertTimeMillis = mysqlInsertTimeZoned.withZoneSameInstant(ZoneOffset.UTC).toInstant().toEpochMilli();
            
            state.diffs.add(timestampMillis + (28800 * 1000) - mysqlInsertTimeMillis);
          }
        """,
        "combine_script": """
          if (state.diffs.size() == 0) {
            return null;
          }
          return state.diffs.stream().mapToLong(Long::longValue).average().orElse(0);
        """,
        "reduce_script": """
          if (states.size() == 0) {
            return null;
          }
          double total = 0;
          int count = 0;
          for (state in states) {
            if (state != null) {
              total += state;
              count++;
            }
          }
          return count == 0 ? null : total / count / 1000;
        """
      }
    }
  }
}

GET /test_index/_search
{
  "size": 0,
  "aggs": {
    "time_diff": {
      "scripted_metric": {
        "init_script": "state.diffs = []",
        "map_script": """
          if (doc['consumeTime'].size() > 0 && doc['mysqlInsertTime'].size() > 0) {
            state.diffs.add(doc['consumeTime'].value.millis - doc['mysqlInsertTime'].value.millis);
          }
        """,
        "combine_script": """
          if (state.diffs.size() == 0) {
            return null;
          }
          return state.diffs.stream().mapToLong(Long::longValue).average().orElse(0);
        """,
        "reduce_script": """
          if (states.size() == 0) {
            return null;
          }
          double total = 0;
          int count = 0;
          for (state in states) {
            if (state != null) {
              total += state;
              count++;
            }
          }
          return count == 0 ? null : total / count / 1000;
        """
      }
    }
  }
}



