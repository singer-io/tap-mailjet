from tap_mailjet.streams.abstracts import IncrementalStream

class ClickStatistics(IncrementalStream):
    tap_stream_id = "click_statistics"
    key_properties = ["ID"]
    replication_method = "INCREMENTAL"
    replication_keys = "ClickedAt"
    data_key = "Data"
    path = "clickstatistics"

