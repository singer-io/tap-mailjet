from tap_mailjet.streams.abstracts import IncrementalStream

class Campaigns(IncrementalStream):
    tap_stream_id = "campaigns"
    key_properties = ["ID"]
    replication_method = "INCREMENTAL"
    replication_keys = ["CreatedAt"]
    data_key = "Data"
    path = "campaign"
    
    def set_incremental_params(self, bookmark_date: str) -> None:
        """Set FromTS parameter for Campaigns stream"""
        # Filter campaigns by creation date
        self.update_params(FromTS=bookmark_date)

