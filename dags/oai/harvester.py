from datetime import datetime
from sickle import Sickle


class OAIHarvester:
    def __init__(self, endpoint_url: str, prefix: str = "oai_dc"):
        self.endpoint = endpoint_url
        self.prefix = prefix
        self.sickle = Sickle(self.endpoint)
        formats = self.sickle.ListMetadataFormats()
        self.supported_formats = [fmt.metadataPrefix for fmt in formats]
        if self.prefix not in self.supported_formats:
            raise ValueError(f"Metadata prefix '{self.prefix}' not supported by endpoint '{self.endpoint}'")


    def identify(self):
        """
        Identify the OAI-PMH endpoint.
        
        Returns:
            Dict: identification info
        """
        identity = self.sickle.Identify()
        return identity

    def get_pid_list(self, from_date: str = None, until_date: str = None):
        """
        Get list of PIDs (and datestamps) from the OAI-PMH endpoint.
        
        Args:
            from_date (str): optional ISO 8601 start date, e.g. "2023-01-01"
            until_date (str): optional ISO 8601 end date, e.g. "2023-12-31"
        
        Returns:
            List[Dict]: list of dicts {"pid": <identifier>, "datestamp": <datestamp>}
        """
        # Pass from/until if provided
        records = self.sickle.ListRecords(**{
            'metadataPrefix': self.prefix,
            'from':from_date,
            'until':until_date
            })

        pids = []
        for record in records:
            pids.append({
                "pid": record.header.identifier,
                "datestamp": self._to_iso(record.header.datestamp),
            })

        return pids

    def _to_iso(self, datestamp: str) -> str:
        """
        Convert a datestamp string from OAI-PMH to ISO 8601 format.
        
        Args:
            datestamp (str): input datestamp, e.g., "2023-07-01" or "2023-07-01T12:34:56Z"
        
        Returns:
            str: ISO 8601 string "YYYY-MM-DDTHH:MM:SSZ"
        """
        # Try parsing date-only
        try:
            dt = datetime.strptime(datestamp, "%Y-%m-%d")
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")  # add time 00:00:00
        except ValueError:
            pass

        # Try parsing datetime with seconds
        try:
            dt = datetime.strptime(datestamp, "%Y-%m-%dT%H:%M:%SZ")
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            pass

        # Add more parsing formats if needed
        raise ValueError(f"Unknown datestamp format: {datestamp}")


        

