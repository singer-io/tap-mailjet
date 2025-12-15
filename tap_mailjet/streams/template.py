from tap_mailjet.streams.abstracts import FullTableStream

class Template(FullTableStream):
    tap_stream_id = "template"
    key_properties = ["ID"]
    replication_method = "FULL_TABLE"
    path = "/v3/REST/template"
    data_key = "Data"
    http_method = "GET"
