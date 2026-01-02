from tap_mailjet.streams.abstracts import FullTableStream

class TopLinkClicked(FullTableStream):
    tap_stream_id = "top_link_clicked"
    key_properties = ["LinkId"]
    replication_method = "FULL_TABLE"
    data_key = "Data"
    path = "toplinkclicked"

