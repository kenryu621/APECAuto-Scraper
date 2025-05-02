import my_libs.utils as Utils
from my_libs.dependencies import *


class ApecData(Enum):
    """
    Enum class representing keys used for extracting and organizing data related to Apec.

    Members:
        KEYWORD
        CATEGORY
        MANUFACTURER
        ARTICLE
        NAME
        NAME_URL
        WEIGHT
        AVAILABILITY
        LEAD_TIME
        INFORMATION
        PRICE

    Usage:
        The `ApecData` enum provides a standardized set of keys for accessing and organizing data in Apec-related spreadsheets.

    Notes:
        - Enum members are used as keys in dictionaries where data is stored and retrieved.
    """

    KEYWORD = DataAttr(header="Keyword", column=0)
    CATEGORY = DataAttr(header="Category", column=1)
    MANUFACTURER = DataAttr(header="Manufacturer", column=2)
    ARTICLE = DataAttr(header="Article", column=3)
    NAME = DataAttr(header="Name", column=4)
    NAME_URL = DataAttr()
    WEIGHT = DataAttr(header="Net Weight, kg", column=5)
    AVAILABILITY = DataAttr(header="Availability", column=6)
    LEAD_TIME = DataAttr(header="Lead Time, days", column=7)
    INFORMATION = DataAttr(header="Information", column=8)
    PRICE = DataAttr(header="Price ($)", column=9)


class MyApecExcel:
    """
    Class for managing an Excel workbook specifically for Apec data.

    Attributes:
        workbook (xlsxwriter.Workbook): The Excel workbook instance.
        formats (dict[FormatType, xlsxwriter.format.Format]): Formatting options for the workbook.
        worksheet (xlsxwriter.worksheet.Worksheet): The worksheet within the workbook.
        row_count (int): Counter for the current row in the worksheet.

    Methods:
        create_workbook(name: str, output_dir: str) -> xlsxwriter.Workbook:
            Create and return a new Excel workbook with the given filename.
        save_workbook() -> None:
            Finalize and save the workbook, with retry logic for permission errors.
        add_headers() -> None:
            Write the header row to the worksheet and update row count.
        write_data_row(data: dict[ApecData, Any]) -> None:
            Write a row of Apec data to the worksheet.
    """

    def __init__(self, name: str, output_dir: str) -> None:
        """
        Initialize a new Excel workbook for Apec data with a specified name and output directory.

        Args:
            name (str): The name used to generate the Excel file's name.
            output_dir (str): The directory where the workbook will be saved.

        Initializes:
            workbook (xlsxwriter.Workbook): The Excel workbook instance created with the specified name.
            formats (dict[FormatType, xlsxwriter.format.Format]): Dictionary of formats for different cell types.
            worksheet (xlsxwriter.worksheet.Worksheet): A worksheet named "Apec Data" added to the workbook.
            row_count (int): Initial row count set to 0, used for tracking the current row in the worksheet.
        """
        self.workbook: xlsxwriter.Workbook = self.create_workbook(name, output_dir)
        self.formats: dict[FormatType, xlsxwriter.format.Format] = initialize_formats(
            self.workbook
        )
        self.worksheet = self.workbook.add_worksheet("Apec Data")
        # self.screenshot_sheet = self.workbook.add_worksheet("Screenshots")
        self.row_count = 0
        self.add_headers()

    def create_workbook(self, name: str, output_dir: str) -> xlsxwriter.Workbook:
        """
        Create a new Excel workbook with a filename based on the provided keyword.

        Args:
            keyword (str): The keyword used to name the Excel file.
            output_directory (str): The directory where the workbook will be saved.

        Returns:
            xlsxwriter.Workbook: The created Workbook instance.
        """
        output_file = os.path.join(output_dir, f"{name}.xlsx")
        logging.info("Creating new workbook at %s", output_file)
        workbook = xlsxwriter.Workbook(output_file, {"in_memory": True})
        # workbook = xlsxwriter.Workbook(output_file)
        logging.info("Workbook created successfully")
        return workbook

    def add_table(self) -> None:
        self.worksheet.add_table(
            0,
            0,
            self.row_count - 1,
            Utils.get_enum_last_col(ApecData),
            {
                "columns": [
                    {"header": header}
                    for header in Utils.get_enum_headers_row(ApecData)
                ],
                "style": None,
            },
        )

    def save_workbook(self) -> None:
        """
        Finalize the worksheet by autofitting columns, and save/close the Excel workbook with retry logic.

        Notes:
            - Autofits columns in the worksheet.
            - Saves and closes the workbook, handling any errors related to file permissions.
            - Logs a message indicating success or an error if the workbook cannot be saved.
            - Retries if the file is open elsewhere, prompting the user to close it and retry.
        """
        self.add_table()
        self.worksheet.autofit()
        while True:
            try:
                self.workbook.close()
                logging.info("Workbook successfully saved.")
                break  # Exit the loop if the workbook is saved successfully
            except OSError as e:
                if e.errno == errno.EACCES:  # Permission denied error
                    logging.error(
                        "PermissionError: Please close the Excel file if it is open and press Enter to retry."
                    )
                    input("Please close the Excel file and press Enter to retry...")
                else:
                    logging.error(
                        "An OSError occurred while saving the workbook: %s", e
                    )
                    input("An unexpected error occurred. Press Enter to retry...")
            except Exception as e:
                logging.error(
                    "An unexpected error occurred while saving the workbook: %s", e
                )
                input("An unexpected error occurred. Press Enter to retry...")

    def add_headers(self) -> None:
        headers = Utils.get_enum_headers_row(ApecData)
        self.worksheet.write_row(
            0, 0, headers, cell_format=self.formats[FormatType.HEADER]
        )
        self.row_count += 1

    def write_data_row(self, data: dict[ApecData, Any], lock: Lock) -> None:
        fields_to_write = [
            (ApecData.KEYWORD, None),
            (ApecData.CATEGORY, None),
            (ApecData.MANUFACTURER, None),
            (ApecData.ARTICLE, None),
            (ApecData.NAME, ApecData.NAME_URL),
            (ApecData.WEIGHT, None),
            (ApecData.AVAILABILITY, None),
            (ApecData.LEAD_TIME, None),
            (ApecData.INFORMATION, None),
            (ApecData.PRICE, None),
        ]

        for data_key, url_key in fields_to_write:
            is_currency = data_key == ApecData.PRICE
            Utils.write_data(
                self.worksheet,
                self.formats,
                self.row_count,
                Utils.get_enum_col(data_key),
                data,
                data_key,
                url_key=url_key,
                is_currency=is_currency,
                lock=lock,
            )

        self.row_count += 1

    # def add_screenshot(self, row_idx: int, file_path: str) -> None:
    #     """
    #     Add a screenshot image to the specified worksheet at the current row index.

    #     Args:
    #         row_idx (int): The row index of the screenshot sheet where the screenshot will be embedded
    #         file_path (str): The file path of the screenshot to insert.
    #     """
    #     self.screenshot_sheet.set_column_pixels(0, 0, 500)
    #     self.screenshot_sheet.set_row_pixels(row_idx, 500)
    #     logging.info(f"Embedding image at row {row_idx + 1}: {file_path}")
    #     self.screenshot_sheet.embed_image(row_idx, 0, file_path)
