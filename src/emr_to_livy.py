from typing import List


def from_dash_to_camel_case(conf_key: str):
    if conf_key[:2] != '--':
        raise ValueError("`%s` is not a --conf param")
    conf_key = conf_key[2:]
    conf_words = conf_key.split('-')
    conf_words = [conf_words[0]] + list(map(lambda w: w.capitalize(), conf_words[1:]))
    return ''.join(conf_words)


def extract_conf_until_jar(livy_step: dict, args: List[str]):
    spark_conf = {}
    it = iter(args)
    for key in it:
        if key.startswith('--'):
            val = next(it)
            camel_key = from_dash_to_camel_case(key)
            if camel_key == 'conf':
                key_val_ls = val.split("=")
                if len(key_val_ls) != 2:
                    raise ValueError("spark --conf a `%s` is badly formatted", val)
                spark_conf[key_val_ls[0]] = key_val_ls[1]
            livy_step[camel_key] = val
        elif '.jar' in key:
            return {**livy_step, 'conf': spark_conf, 'args': list(it), 'file': key}
        else:
            raise ValueError("Emr step is not of expected format %s", args)


def transform_emr_to_livy(cli_args):
    livy_step = {}
    if 'spark-submit' in cli_args[0]:
        livy_step['kind'] = 'spark'
    else:
        raise ValueError("Unsupported command `%s`", cli_args[0])

    return extract_conf_until_jar(livy_step, cli_args[1:])
