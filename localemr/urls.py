from __future__ import unicode_literals
from localemr.responses import LocalElasticMapReduceResponse

url_bases = [
    "https?://(.+).elasticmapreduce.amazonaws.com",
    "https?://elasticmapreduce.(.+).amazonaws.com",
]

url_paths = {"{0}/$": LocalElasticMapReduceResponse.dispatch}
