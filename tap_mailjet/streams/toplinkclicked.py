from tap_mailjet.streams.abstracts import FullTableStream

class Toplinkclicked(FullTableStream):
    tap_stream_id = "toplinkclicked"
    key_properties = ["LinkId"]
    replication_method = "FULL_TABLE"
    path = "/v3/REST/toplinkclicked"
    data_key = "Data"
    http_method = "GET"
