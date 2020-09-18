# Scripts to etl data from hail table to elasticsearch for IBD browser

We made changes for IBDGC dataset based on Broad Institute orginal gnomad browser ETL code https://github.com/broadinstitute/gnomad-browser/tree/master/data
The code can be used to greate `genes` and `exomes` indexes in elasticsearch.

```
EXAMPLE
python3 /home/ubuntu/ibdgc-tools/hailtable-etl/hail_to_es.py /home/
ubuntu/dumpdata/EUR800k1.ht  http://localhost:9200 ditest_exome --id-field variant_id
```
