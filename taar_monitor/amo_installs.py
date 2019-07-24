from .redash_base import AbstractData, build_params, STMO_API_KEY


class AddonInstallEvents(AbstractData):
    QUERY_ID = 63769

    def get_install_events(self, tbl_date):
        row_iter = self._get_raw_data(tbl_date)
        return list(row_iter)

    def _get_raw_data(self, tbl_date):
        """
        Yield 3-tuples of (sha256 hashed client_id, guid, timestamp)
        """
        results = self._query_redash(tbl_date)

        for row in results:
            yield (row)

    def _query_redash(self, tbl_date):
        """
        This splices up the query to the redash table into 24 hour long slices
        to reduce the chance that we exceed the maximum row count in a resultset
        """
        iso_date = tbl_date.strftime("'%Y-%m-%d'")
        params = build_params(dt=iso_date)
        data = self.get_fresh_query_result(
            "https://sql.telemetry.mozilla.org", self.QUERY_ID, STMO_API_KEY, params
        )
        return data
