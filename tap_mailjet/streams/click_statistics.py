from tap_mailjet.streams.abstracts import IncrementalStream

class ClickStatistics(IncrementalStream):
    tap_stream_id = "click_statistics"
    key_properties = ["ID"]
    replication_method = "INCREMENTAL"
    replication_keys = ["ClickedAt"]
    data_key = "Data"
    path = "clickstatistics"
    
    def set_incremental_params(self, bookmark_date: str) -> None:
        """Set FromTS parameter for ClickStatistics stream"""
        self.update_params(FromTS=bookmark_date)

