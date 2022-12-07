from copy import copy
import re

from osbenchmark import exceptions
from osbenchmark.workload import loader


def reindex(es, params):
    result = es.reindex(body=params.get("body"), request_timeout=params.get("request_timeout"))
    return result["total"], "docs"


async def reindex_async(es, params):
    result = await es.reindex(body=params.get("body"), request_timeout=params.get("request_timeout"))
    return result["total"], "docs"


# PUT
def create_policy(es, params):
    policy_doc = '''
    {
        "description": "ingesting logs",
        "default_state": "ingest",
        "states": [
              {
                "name": "ingest",
                "actions": [
                  {
                    "rollover": {
                      "min_doc_count": 5000
                    }
                  }
                ],
                "transitions": [
                  {
                    "state_name": "search"
                  }
                ]
              },
              {
                "name": "search",
                "actions": [],
                "transitions": [
                  {
                    "state_name": "delete",
                    "conditions": {
                      "min_index_age": "5m"
                    }
                  }
                ]
              },
              {
                "name": "delete",
                "actions": [
                  {
                    "delete": {}
                  }
                ],
                "transitions": []
              }
            ]
    }
    '''
    result = es.transport.perform_request(
        "PUT", "/_opendistro/_ism/policies/policy_1", params=params, body=policy_doc
    )
    return result['policy_id']


def apply_policy_on_index(es, params):
    policy_id = create_policy(es, params)
    result = es.transport.perform_request("POST", "/_opendistro/_ism/add/logs-181998", params=params, body={
        "policy_id": {{policy_id}}
    })
    return result


def register(registry):
    registry.register_runner("add_policy_on_index", apply_policy_on_index, async_runner=True)
    try:
        registry.register_workload_processor(loader.DefaultWorkloadPreparator())
    except TypeError as e:
        if e == "__init__() missing 1 required positional argument: 'cfg'":
            pass
