from __future__ import annotations

import datetime
import re
from typing import Any

from dfinity.rollout_types import (
    FeatureName,
    RolloutFeatures,
    SubnetId,
)

from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.providers.google.suite.operators.sheets import (
    GoogleSheetsCreateSpreadsheetOperator,
)


def convert_rollout_week_sheet_name(date_with_week_number: str) -> datetime.date:
    day_without_year = re.search(
        "(Jan|Feb|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) ([0-9]+)", date_with_week_number
    )
    day_with_year = re.search(
        "(Jan|Feb|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) ([0-9]+) ([0-9][0-9][0-9][0-9])",
        date_with_week_number,
    )
    if day_with_year:
        thedate = datetime.datetime.strptime(day_with_year.string, "%b %d %Y").date()
    elif day_without_year:
        # Year was not specified
        thedate = datetime.datetime.strptime(
            day_without_year.string + " 2024", "Week %W: %b %d %Y"
        ).date()
    else:
        raise ValueError(f"No legible date in {date_with_week_number}")
    return thedate


def convert_sheet_data_into_feature_subnet_map(
    sheet_data: list[list[Any]],
) -> dict[SubnetId, FeatureName]:
    headings, rows = sheet_data[0], sheet_data[1:]
    if not headings:
        raise Warning("No headings on sheet, ignoring")
    feature_names = headings[1:]
    subnet_id_feature_map: dict[SubnetId, FeatureName] = {}
    for row in rows:
        subnet, feature_requests = row[0], row[1:]
        if not subnet:
            # Empty first cell in the row means no subnet, ignoring.
            continue
        if not feature_requests:
            # No feature requests for this subnet.
            continue
        col_index = [idx for idx, val in enumerate(feature_requests) if val == "yes"]
        if not col_index:
            # None of the columns is marked yes.  No feature request.
            continue
        if len(col_index) > 1:
            raise ValueError(
                f"Sheet contains an invalid 'yes' for subnet {subnet}"
                " under two different features"
            )
        try:
            feature_name = feature_names[col_index[0]]
        except IndexError:
            # There is a yes in the row, but no feature named.
            raise ValueError(
                f"Sheet contains an invalid 'yes' for subnet {subnet}"
                " that has no feature name in the heading"
            )
        if not feature_name:
            raise ValueError(
                f"Sheet contains an invalid 'yes' for subnet {subnet}"
                " that has an empty feature name in the heading"
            )
        subnet_id_feature_map[subnet] = feature_name
    return subnet_id_feature_map


class GetFeatureRolloutPlan(GoogleSheetsCreateSpreadsheetOperator):
    """
    Gets all the rollout information from the sheets.

    `__init__` takes the same parameters as `GoogleSheetsCreateSpreadsheetOperator`.
    """

    def execute(self, context: Any) -> RolloutFeatures:  # type: ignore
        hook = GSheetsHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        spreadsheet_data = hook.get_spreadsheet(
            spreadsheet_id=self.spreadsheet["spreadsheetId"]
        )

        rollout_features: RolloutFeatures = []
        for sheet in spreadsheet_data["sheets"]:
            props = sheet["properties"]
            if props["sheetType"] != "GRID":
                continue
            title = props["title"]
            try:
                date = convert_rollout_week_sheet_name(title)
            except ValueError as e:
                self.log.warn("Ignoring sheet %s: cannot fathom date: %s", title, e)
                continue
            row_count = props["gridProperties"]["rowCount"]
            col_count = props["gridProperties"]["columnCount"]
            self.log.info("Retrieving values of sheet %s for date %s", title, date)
            values = hook.get_values(
                spreadsheet_id=self.spreadsheet["spreadsheetId"],
                range_=f"{title}!R1C1:R{row_count}C{col_count}",
            )
            try:
                subnet_id_feature_map = convert_sheet_data_into_feature_subnet_map(
                    values
                )
            except Warning as w:
                self.log.warn("Ignoring sheet %s: %s", title, w)
            rollout_features.append(
                {
                    "date": date,
                    "subnet_id_feature_map": subnet_id_feature_map,
                }
            )
        return rollout_features


if __name__ == "__main__":
    import sys

    spreadsheet_id = sys.argv[1]
    kn = GetFeatureRolloutPlan(
        task_id="x",
        spreadsheet={"spreadsheetId": sys.argv[1]},
    )
    kn.execute({})
