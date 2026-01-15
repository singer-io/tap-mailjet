from tap_mailjet.streams.abstracts import FullTableStream

class Template(FullTableStream):
    tap_stream_id = "template"
    key_properties = ["ID"]
    replication_method = "FULL_TABLE"
    data_key = "Data"
    path = "template"

