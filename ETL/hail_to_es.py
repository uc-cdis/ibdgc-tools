import argparse
import datetime
import json

import elasticsearch
from elasticsearch import helpers
import hail as hl


HAIL_TYPE_TO_ES_TYPE_MAPPING = {
    hl.tint: "integer",
    hl.tint32: "integer",
    hl.tint64: "long",
    hl.tfloat: "double",
    hl.tfloat32: "float",
    hl.tfloat64: "double",
    hl.tstr: "keyword",
    hl.tbool: "boolean",
}


# https://hail.is/docs/devel/types.html
# https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html
def _elasticsearch_mapping_for_hail_type(dtype):
    if isinstance(dtype, hl.tstruct):
        return {"properties": {field: _elasticsearch_mapping_for_hail_type(dtype[field]) for field in dtype.fields}}

    if isinstance(dtype, (hl.tarray, hl.tset)):
        element_mapping = _elasticsearch_mapping_for_hail_type(dtype.element_type)

        if isinstance(dtype.element_type, hl.tstruct):
            element_mapping["type"] = "nested"

        return element_mapping

    if isinstance(dtype, hl.tlocus):
        return {"type": "object", "properties": {"contig": {"type": "keyword"}, "position": {"type": "integer"}}}

    if dtype in HAIL_TYPE_TO_ES_TYPE_MAPPING:
        return {"type": HAIL_TYPE_TO_ES_TYPE_MAPPING[dtype]}

    # tdict, ttuple, tinterval, tcall
    raise NotImplementedError


def _set_field_parameter(mapping, field, parameter, value):
    keys = field.split(".")
    ref = mapping
    for key in keys:
        ref = ref["properties"][key]

    ref[parameter] = value


def elasticsearch_mapping_for_table(table, disable_fields=None, override_types=None):
    """
    Creates an Elasticsearch mapping definition for a Hail table's row value type.

    https://www.elastic.co/guide/en/elasticsearch/guide/current/root-object.html
    """
    mapping = _elasticsearch_mapping_for_hail_type(table.key_by().row_value.dtype)

    if disable_fields:
        for field in disable_fields:
            _set_field_parameter(mapping, field, "enabled", False)

    if override_types:
        for field, field_type in override_types.items():
            _set_field_parameter(mapping, field, "type", field_type)

    return mapping


def build_bulk_request(documents, index_name, id_field=None):
    if id_field:
        return [{"_index": index_name, "_id": d[id_field], "_source": d} for d in documents]
    else:
        return [{"_index": index_name, "_source": d} for d in documents]


def export_table_to_elasticsearch(
    table, host, index_name, block_size=5000, id_field=None, mapping=None, num_shards=10, port=9200, verbose=True
):
    es_client = elasticsearch.Elasticsearch(host, port=port)

    if not mapping:
        mapping = elasticsearch_mapping_for_table(table)

    # Delete the index before creating it
    if es_client.indices.exists(index=index_name):
        es_client.indices.delete(index=index_name)

    mapping["_meta"] = dict(hl.eval(table.globals))

    # https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules.html#index-modules-settings
    request_body = {
        "mappings": mapping,
        "settings": {
            "index.codec": "best_compression",
            "index.mapping.total_fields.limit": 10000,
            "index.number_of_replicas": 0,
            "index.number_of_shards": num_shards,
            "index.refresh_interval": -1,
        },
    }

    es_client.indices.create(index=index_name, body=request_body)

    temp_file = "table-tmp.json.txt"
    table = table.key_by()
    table.select(json=hl.json(table.row_value)).export(temp_file, header=False)

    buffer = []
    with open(temp_file) as f:
        for line in f:
            data = json.loads(line)
            buffer.append(data)

            if len(buffer) >= block_size:
                helpers.bulk(
                    es_client,
                    build_bulk_request(buffer, index_name, id_field)
                )
                buffer = []

    if buffer:
        helpers.bulk(
            es_client,
            build_bulk_request(buffer, index_name, id_field)
        )
        buffer = []

    es_client.indices.forcemerge(index=index_name)



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("table_url", help="URL of Hail table to export")
    parser.add_argument("host", help="Elasticsearch host or IP")
    parser.add_argument("index_name", help="Elasticsearch index name")
    parser.add_argument("--block-size", help="Elasticsearch block size to use when exporting", default=200, type=int)
    parser.add_argument("--disable-fields", help="Disable a field in Elasticsearch", action="append", default=[])
    parser.add_argument("--id-field", help="Field to use as Elasticsearch document ID", default=None)
    parser.add_argument("--num-shards", help="Number of elasticsearch shards", default=1, type=int)
    parser.add_argument("--port", help="Elasticsearch port", default=9200, type=int)
    parser.add_argument("--set-type", help="Set a specific Elasticsearch type for a field", action="append", default=[])
    args = parser.parse_args()

    hl.init(log="/tmp/hail.log")

    table = hl.read_table(args.table_url)

    table = table.select_globals(
        exported_from=args.table_url,
        exported_at=datetime.datetime.utcnow().isoformat(timespec="seconds"),
    )

    mapping = elasticsearch_mapping_for_table(
        table, disable_fields=args.disable_fields, override_types=dict(arg.split("=") for arg in args.set_type)
    )

    export_table_to_elasticsearch(
        table,
        host=args.host,
        index_name=args.index_name,
        block_size=args.block_size,
        id_field=args.id_field,
        mapping=mapping,
        num_shards=args.num_shards,
        port=args.port,
        verbose=True,
    )


if __name__ == "__main__":
    main()