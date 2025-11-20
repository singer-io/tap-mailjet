from tap_mailjet.streams.abstracts import FullTableStream

class Contactdata(FullTableStream):
    tap_stream_id = "contactdata"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    path = "/v3/REST/contactdata"

