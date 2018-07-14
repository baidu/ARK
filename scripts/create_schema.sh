#!/bin/bash

ES_HOST=${ARK_ES_HOST}
ES_PORT=${ARK_ES_PORT}

curl -XPUT "${ES_HOST}:${ES_PORT}/ark"

curl -XPUT "${ES_HOST}:${ES_PORT}/ark/_mapping/operation" -d '{
        "_all": {
            "enabled": false
        },
        "dynamic": false,
        "properties": {
            "status": {
                "index": "not_analyzed",
                "type": "string"
            },
            "operation_id": {
                "index": "not_analyzed",
                "type": "string"
            },
            "actions": {
                "type": "object"
            },
            "operation_params": {
                "type": "object"
            },
            "periods": {
                "type": "object"
            }
        }
}'
