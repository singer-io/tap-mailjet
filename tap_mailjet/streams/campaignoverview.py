from tap_mailjet.streams.abstracts import FullTableStream

class Campaignoverview(FullTableStream):
    tap_stream_id = "campaignoverview"
    key_properties = ["ID"]
    replication_method = "FULL_TABLE"
    path = "/v3/REST/campaignoverview"
    data_key = "Data"
    http_method = "GET"
