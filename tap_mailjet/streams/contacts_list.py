from tap_mailjet.streams.abstracts import FullTableStream

class ContactsList(FullTableStream):
    tap_stream_id = "contacts_list"
    key_properties = ["ID"]
    replication_method = "FULL_TABLE"
    data_key = "Data"
    path = "contactslist"

