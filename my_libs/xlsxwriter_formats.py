from enum import Enum, auto
from typing import Optional

import xlsxwriter
import xlsxwriter.format
from attr import dataclass


@dataclass
class DataAttr():
    header: Optional[str] = None
    column: Optional[int] = None


class FormatType(Enum):
    """
    Enum class representing the different types of formatting options for data in Excel sheets.

    Members:
        HEADER: Formatting style for header cells in Excel sheets.
        DATE: Formatting style for cells containing date values.
        NUMBER: Formatting style for cells containing numeric values.
        ACCOUNTING: Formatting style for cells displaying monetary values.

    Usage:
        The `FormatType` enum provides standardized formatting options for different types of data when writing to Excel sheets. Each member of the enum corresponds to a specific formatting style that can be applied to cells to ensure consistency and clarity in the presentation of data.

    Notes:
        - Enum members can be used to apply appropriate formatting styles when generating Excel reports or outputs.
    """

    HEADER = auto()
    DATE = auto()
    NUMBER = auto()
    FLOAT = auto()
    CURRENCY = auto()
    FILL = auto()
    FILL_URL = auto()


def initialize_formats(
    workbook: xlsxwriter.Workbook,
) -> dict[FormatType, xlsxwriter.format.Format]:
    """
    Initialize common formats for Excel sheets.

    Args:
        workbook (xlsxwriter.Workbook): The workbook to add formats to.

    Returns:
        (dict[FormatType, xlsxwriter.format.Format]): A dictionary of formats.
    """
    formats: dict[FormatType, xlsxwriter.format.Format] = {
        FormatType.HEADER: workbook.add_format({"bold": True}),
        FormatType.DATE: workbook.add_format({"num_format": "mm/dd/yyyy"}),
        FormatType.NUMBER: workbook.add_format({"num_format": 3}),
        FormatType.FLOAT: workbook.add_format({"num_format": "General"}),
        FormatType.CURRENCY: workbook.add_format({"num_format": "$* #,##0.00"}),
        FormatType.FILL: workbook.add_format({"bg_color": "#daf2d0"}),
        FormatType.FILL_URL: workbook.add_format(
            {"bg_color": "#daf2d0", "font_color": "blue", "underline": True}
        ),
    }
    return formats
