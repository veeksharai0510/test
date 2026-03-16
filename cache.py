part1:

import os
import redis
class RedisConfig:
    def __init__(self, host, port, password):
        self.RC_HOST = host
        self.RC_PORT = port
        self.RC_PASS = password
 
    def redis_as_dict(self):
        return {
            "RC_HOST": self.RC_HOST,
            "RC_PORT": self.RC_PORT,
            "RC_PASS": self.RC_PASS
        }
 
    @property
    def redis_client(self):
        return redis.Redis(
            host=self.RC_HOST,
            port=self.RC_PORT,
            password=self.RC_PASS,
            decode_responses=True
        )
 
 

cre_config = RedisConfig(
    host=os.getenv("CRE_RC_HOST"),
    port=os.getenv("CRE_RC_PORT"),
)







part2:

import json

from app.utils.log_config import setup_logger
from app.core.custom_exceptions import NoDataException
from sqlalchemy.engine import RowMapping

logger = setup_logger(__name__)

def normalize_for_json(obj):
   if isinstance(obj, RowMapping):
       return dict(obj)
   if hasattr(obj, "model_dump"):  # Pydantic v2
       return obj.model_dump()
   if isinstance(obj, dict):
       return {k: normalize_for_json(v) for k, v in obj.items()}
   if isinstance(obj, list):
       return [normalize_for_json(i) for i in obj]
   return obj

def set_cache_(data_dict,redis_client,data):
    try:
        orch_config = data_dict.get("orch_config")
        if not orch_config:
            raise ValueError("orch_config not found in data")
        flow_components = orch_config["data"]["pipeline_details"]["flow"][
            "flow_components"
        ]

        for node_name, node_data in flow_components.items():
            node_id = node_data.get("node_id")

            if node_id == "NA" or node_id is None:
                continue

            flat_node = {
                k: json.dumps(v) if isinstance(v, (dict, list)) else str(v)
                for k, v in node_data.items()
            }
            node_key = f"CRE_AGV{data.version}_UC_{data.usecase_code}_{node_id}"
            existing_data = redis_client.hgetall(node_key)
            logger.info(f"node key is {node_key}")
            # Compare and find changed or new keys
            dirty_update = {
                k: v for k, v in flat_node.items()
                if existing_data.get(k.encode()) != v.encode()
            }
            if dirty_update:
                redis_client.hset(node_key, mapping=dirty_update)
                logger.info(f"Updated fields: {dirty_update}")
            else:
                logger.info("No changes detected.")
    except Exception as e:
        logger.info(f"Error in set cache sub function {e}")
        raise e

def set_cache(key: str, data, redis_client) -> None:
    try:
        if hasattr(data, "model_dump"):
            data_dict = data.model_dump()
        else:
            data_dict=data
        normalized_data = normalize_for_json(data_dict)
        data_str = {
            k: json.dumps(v) if isinstance(v, (dict, list)) else str(v)
            for k, v in normalized_data.items()
        }
        existing_data = redis_client.hgetall(key)
        
        # Compare and find changed or new keys
        dirty_update = {
            k: v for k, v in data_str.items()
            if existing_data.get(k.encode()) != v.encode()
        }

        if dirty_update:
            redis_client.hset(key, mapping=dirty_update)
            logger.info(f"Updated fields: {dirty_update}")
        else:
            logger.info("No changes detected.")

        if data_dict.get("orch_config"):
            set_cache_(data_dict,redis_client,data)
    except Exception as e:
       logger.info(f"Redis set_cache error: {e}")
       raise e


def get_cache(key: str, redis_client, auth=False):
    try:
        logger.info("inside get cache")
        if not redis_client.exists(key):
            raise NoDataException

        # Retrieve all hash fields for the given key
        cached_data = redis_client.hgetall(key)
        if not cached_data:
            raise NoDataException
        interface_schema = json.loads(cached_data["interface"])
        agent_id=cached_data["agent_id"]
        agent_version=cached_data["version"]
        agent_config= cached_data["agent_config"]
        is_chat_enabled= cached_data["isChatEnabled"]
        agent_type= cached_data["agent_type"]
        api_key= cached_data["api_key"]
        if auth:
            agent_code = cached_data['agent_code']
            return interface_schema,agent_id,agent_version,agent_type,agent_config,is_chat_enabled,api_key,agent_code
        
        return interface_schema,agent_id,agent_version,agent_type,agent_config,is_chat_enabled,api_key
    except Exception as e:
        logger.info(f"Redis set_cache error: {e}")
        raise e


part3:
class CacheData(BaseModel):
    agent_id: Optional[int] = None
    usecase_code: Optional[str] = None
    version: Optional[int] = None

key=''
redis_client = get_redis_client(type)
cache_data = CacheData(
            agent_id=result["AD_ID"],
            agent_code=result["AD_AGENT_CODE"],
            usecase_code=result["AD_USECASE_CODE"])
set_cache(key, cache_data, redis_client)
get_cache(key, redis_client)
