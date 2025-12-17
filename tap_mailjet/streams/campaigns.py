from tap_mailjet.streams.abstracts import IncrementalStream

class Campaigns(IncrementalStream):
    tap_stream_id = "campaigns"
    key_properties = ["ID"]
    replication_method = "INCREMENTAL"
    replication_keys = ["CreatedAt"]
    data_key = "Data"
    path = "campaign"

