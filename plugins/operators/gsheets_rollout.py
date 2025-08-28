from __future__ import annotations

import datetime
from logging import Logger
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


def convert_rollout_week_sheet_name(sheet_name: str) -> datetime.date:
    try:
        thedate = datetime.datetime.strptime(sheet_name, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"No legible date in {sheet_name}")
    return thedate


def convert_sheet_data_into_feature_subnet_map(
    sheet_data: list[list[Any]], log: Logger
) -> dict[SubnetId, FeatureName]:
    if not sheet_data or not sheet_data[0]:
        raise Warning("No headings in the provided sheet data, ignoring")

    headings, rows = sheet_data[0], sheet_data[1:]
    feature_names = [x.strip() for x in headings[1:]]  # Skip the subnet column
    subnet_id_feature_map: dict[SubnetId, FeatureName] = {}

    for index, row in enumerate(rows):
        log.info("Processing row %s: %s", index, row)

        # Strip whitespaces from each cell, and convert to lowercase
        row = [cell.strip().lower() for cell in row]

        # Ensure there are enough columns
        if len(row) < 2:
            log.warning("Row %s has insufficient data, skipping", index)
            continue

        subnet, feature_values = row[0], row[1:]

        # Skip rows with an empty subnet cell
        if not subnet:
            log.warning("Row %s has no subnet specified, skipping", index)
            continue

        # Normalize and filter the feature values
        feature_values = [value for value in feature_values if value]
        # Find the indices of enabled features
        enabled_indices = [
            idx for idx, val in enumerate(feature_values) if val in {"yes", "true"}
        ]
        if not feature_values or not enabled_indices:
            log.info("Row %s has no enabled features for subnet %s", index, subnet)
            continue

        # Check if any invalid values exist
        invalid_values = [
            val for val in feature_values if val not in {"true", "false", "yes", "no"}
        ]
        if invalid_values:
            raise ValueError(
                f"Invalid values {invalid_values} in row {index} for subnet {subnet}"
            )

        # Validate if only one feature is enabled
        if len(enabled_indices) != 1:
            raise ValueError(
                f"Subnet {subnet} has {len(enabled_indices)} enabled features; "
                "expected exactly one"
            )

        # Retrieve the enabled feature name
        enabled_feature = feature_names[enabled_indices[0]]
        if not enabled_feature:
            raise ValueError(
                f"Subnet {subnet} has an enabled feature with an empty name"
            )

        # Map the subnet to the enabled feature
        subnet_id_feature_map[subnet] = enabled_feature

    return subnet_id_feature_map


class GetFeatureRolloutPlan(GoogleSheetsCreateSpreadsheetOperator):
    """
    Gets all the rollout information from the sheets.

    `__init__` takes the same parameters as `GoogleSheetsCreateSpreadsheetOperator`.
    """

    def execute(self, context: Any) -> RolloutFeatures:  # type: ignore
        hook = GSheetsHook(
            gcp_conn_id=self.gcp_conn_id,
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
                self.log.warning("Ignoring sheet %s: cannot parse date: %s", title, e)
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
                    values, self.log
                )
            except Warning as w:
                self.log.warning("Ignoring sheet %s: %s", title, w)
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
