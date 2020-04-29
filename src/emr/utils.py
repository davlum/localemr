from __future__ import unicode_literals
import random
import string

import six


def random_id(size=13):
    chars = list(range(10)) + list(string.ascii_uppercase)
    return "".join(six.text_type(random.choice(chars)) for x in range(size))


def random_cluster_id(size=13):
    return "j-{0}".format(random_id())


def random_step_id(size=13):
    return "s-{0}".format(random_id())


def random_instance_group_id(size=13):
    return "i-{0}".format(random_id())


def steps_from_query_string(querystring_dict):
    steps = []
    for step in querystring_dict:
        step["jar"] = step.pop("hadoop_jar_step._jar")
        step["properties"] = dict(
            (o["Key"], o["Value"]) for o in step.get("properties", [])
        )
        step["args"] = []
        idx = 1
        keyfmt = "hadoop_jar_step._args.member.{0}"
        while keyfmt.format(idx) in step:
            step["args"].append(step.pop(keyfmt.format(idx)))
            idx += 1
        steps.append(step)
    return steps


def tags_from_query_string(
    querystring_dict, prefix="Tag", key_suffix="Key", value_suffix="Value"
):
    response_values = {}
    for key, value in querystring_dict.items():
        if key.startswith(prefix) and key.endswith(key_suffix):
            tag_index = key.replace(prefix + ".", "").replace("." + key_suffix, "")
            tag_key = querystring_dict.get(
                "{prefix}.{index}.{key_suffix}".format(
                    prefix=prefix, index=tag_index, key_suffix=key_suffix,
                )
            )[0]
            tag_value_key = "{prefix}.{index}.{value_suffix}".format(
                prefix=prefix, index=tag_index, value_suffix=value_suffix,
            )
            if tag_value_key in querystring_dict:
                response_values[tag_key] = querystring_dict.get(tag_value_key)[0]
            else:
                response_values[tag_key] = None
    return response_values