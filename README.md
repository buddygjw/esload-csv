# esload-csv
Elasticsearch Loader is the tool that batch load CSV file into elasticsearch on high-performance with disruptor framework.

#elasticsearch configuration
```
es.clustername=elasticsearch
es.address=127.0.0.1:9300

#index data
csv.folder=C:\\your-work-space\\zhaoch\\testdata\\a,C:\\your-work-space\\zhaoch\\testdata\\b
csv.poll=10m
csv.filename.extensions=csv
csv.field.separator=;
csv.charset=gbk

#bulk proccess
bulk.size=1000
bulk.concurrent.requests=12
bulk.flush.interval=5s

#whole configure
carrier.total.count=1  
```
