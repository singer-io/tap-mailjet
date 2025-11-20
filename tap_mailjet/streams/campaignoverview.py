from tap_mailjet.streams.abstracts import FullTableStream

class Campaignoverview(FullTableStream):
    tap_stream_id = "campaignoverview"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    path = "/v3/REST/campaignoverview"

