from tap_mailjet.streams.abstracts import FullTableStream

class Campaign(FullTableStream):
    tap_stream_id = "campaign"
    key_properties = ["ID"]
    replication_method = "FULL_TABLE"
    path = "/v3/REST/campaign"
    data_key = "Data"
    http_method = "GET"

