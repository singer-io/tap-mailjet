from tap_mailjet.streams.abstracts import FullTableStream

class Contactslist(FullTableStream):
    tap_stream_id = "contactslist"
    key_properties = ["ID"]
    replication_method = "FULL_TABLE"
    path = "/v3/REST/contactslist"
    data_key = "Data"
    http_method = "GET"

