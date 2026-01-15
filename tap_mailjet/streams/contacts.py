from tap_mailjet.streams.abstracts import FullTableStream

class Contacts(FullTableStream):
    tap_stream_id = "contacts"
    key_properties = ["ID"]
    replication_method = "FULL_TABLE"
    data_key = "Data"
    path = "contact"

