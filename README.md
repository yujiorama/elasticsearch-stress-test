# Elasticsearch Stress Test

### Overview
This script generates a bunch of documents, and indexes as much as it can to elasticsearch. While doing so, it prints out metrics to the screen to let you follow how your cluster is doing.

### How to use
* Save this script
* Make sure you have python 2.7+
* `pip install elasticsearch`

### How does it work
The script creates document templates based on your input. Say - 5 different documents.
The documents are created without fields, for the purpose of having the same mapping when indexing to ES.
After that, the script takes 10 random documents out of the template pool (with redraws) and populates them with random data.

After we have the pool of different documents, we select an index out of the pool, select documents * bulk size out of the pool, and index them.

The generation of documents is being processed before the run, so it will not overload the server too much during the benchmark.

### Mandatory Parameters

| パラメータ                  | 説明           |
|:---------------------------:|:--------------:|
| `--indices`                 | インデックス数 |
| `--number_of_shards`        | プライマリシャード数 |
| `--number_of_replicas`      | 1ノードなので0 |
| `--documents`               | フィールドの組み合わせのバリエーション数。 |
| `--max_fields_per_document` | 1 ドキュメントのフィールド数 |
| `--max_size_per_field`      | フィールドあたりのデータ長 |
| `--bulk_clients`            | bulk リクエストを送信し続けるスレッド数 |
| `--bulk_size`               | bulk リクエストのドキュメント数 |
| `--search_clients`          | search リクエストを送信し続けるスレッド数 |
| `--search_result_size`      | 検索結果サイズ |
| `--seconds`                 | ざっくりの継続時間 |
| `--stats_frequency` | How frequent to show the statistics |30|
| `--no_cleanup` | Boolean field. Don't delete the indices after completion |False|
| `--not_green` | Script doesn't wait for the cluster to be green |False|


### Examples

1. `test.conf` を参考にパラメータを設定する
2. `test` スクリプトを実行する ( `TEST_CONFIG=/path/to/test.conf test` )
3. 実行結果を確認する ( `test_1_10.log` )

### Contribution

You are more then welcome!
Please open a PR or issues here.

