import copy
import logging
logger = logging.getLogger(__name__)

def buildSwaggerFrom(schema):
    ret = {}

    for key, value in schema.items():
        val = copy.deepcopy(value)

        if val["type"] == "string":
            ret[key] = val
        elif val["type"] == "list":
            val["type"] = "array"
            val["items"] = val["schema"]
            val.pop("schema", None)
            ret[key] = val
        else:
            raise NotImplementedError("This type is not implemented in buildSwagger yet!")

    return {
        "type": "object",
        "properties": ret
    }