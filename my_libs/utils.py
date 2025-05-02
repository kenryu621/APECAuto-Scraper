import logging
import os
from datetime import datetime, timedelta
from enum import Enum
from io import BytesIO
from threading import Lock
from typing import Optional, Type
from urllib.parse import urlencode, urljoin

import requests
import xlsxwriter
import xlsxwriter.format
import xlsxwriter.worksheet
from PIL import Image
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from my_libs.xlsxwriter_formats import DataAttr, FormatType


def get_output_directory(base_folder: str) -> str:
    """
    Create or retrieve the output directory for today's date within the base folder.

    Args:
        base_folder (str): The base directory to use for output.

    Returns:
        str: The path to the output directory.
    """
    if not base_folder.endswith(os.path.sep):
        base_folder += os.path.sep

    today = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_directory = os.path.join(base_folder, today)
    os.makedirs(output_directory, exist_ok=True)
    logging.debug("Output directory '%s' created or already exists.", output_directory)

    return output_directory


def create_subfolder(parent_folder_path: str, child_folder_name: str) -> str:
    """
    Create or retrieve the folder for storing images within the base folder.

    Args:
        output_dir (str): The base directory to use for images.

    Returns:
        str: The path to the image folder.
    """
    child_folder_path = os.path.join(parent_folder_path, f"{child_folder_name}")
    os.makedirs(child_folder_path, exist_ok=True)
    logging.debug("Image folder '%s' created or already exists.", child_folder_path)
    return child_folder_path


def delete_folder(folder_path: str) -> None:
    import shutil

    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
        logging.info("Deleted image folder: %s", folder_path)


def download_image(
    image_url: str, save_directory: str, image_name: str
) -> Optional[str]:
    """
    Download an image from a URL and save it to the specified directory.

    Args:
        image_url (str): The URL of the image to download.
        save_directory (str): The directory where the image should be saved.
        image_name (str): The name to use for the saved image file.

    Returns:
        Optional[str]: The path to the saved image file, or None if an error occurred.
    """
    try:
        # Determine the image format from the URL (lowercase for consistency)
        image_format = image_url.split(".")[-1].lower()

        # Set the output path with the correct extension
        if image_format == "webp":
            # Convert WebP images to PNG for compatibility
            output_path = os.path.join(save_directory, f"{image_name}.png")
        else:
            output_path = os.path.join(save_directory, f"{image_name}.{image_format}")

        logging.debug("Downloading image from %s", image_url)

        # Download the image
        response = requests.get(image_url)
        response.raise_for_status()  # Ensure we notice HTTP errors
        image = Image.open(BytesIO(response.content))

        # Convert to RGB if necessary and save
        if image_format in ("jpg", "jpeg"):
            if image.mode in ("RGBA", "P"):
                image = image.convert("RGB")
            image.save(output_path, "JPEG")
        else:
            image.save(
                output_path, "PNG" if image_format == "webp" else image_format.upper()
            )

        logging.debug("Image saved successfully as %s", output_path)
        return output_path

    except Exception as e:
        logging.error("An error occurred while downloading the image: %s", e)
        return None


def build_terapeak_url(
    search_keyword: str, day_range: int = 30, offset: Optional[int] = 0
) -> str:
    """
    Build the eBay URL for the given search keyword.

    Args:
        search_keyword (str): The search keyword.
        day_range (int): The search day range, default is 30.

    Returns:
        str: The constructed eBay URL.
    """
    start_date, end_date = calculate_ebay_dates(day_range)

    # Base URL for Terapeak research
    base_url = "https://www.ebay.com/sh/research"

    if not search_keyword:
        raise ValueError("Keyword must be provided for the search.")

    # Query parameters
    params = {
        "marketplace": "EBAY-US",
        "keywords": search_keyword.replace(" ", "+"),
        "dayRange": day_range,
        "endDate": end_date,
        "startDate": start_date,
        "conditionId": "1000",
        "buyerCountry": "BuyerLocation:::US",
        "offset": offset,
        "limit": 50,
        "tabName": "SOLD",
    }

    # Construct the query string
    query_string = urlencode(params)

    # Construct the full URL using urljoin
    return urljoin(base_url, f"?{query_string}")


def build_ebay_search_url(search_keyword: str) -> str:
    """
    Build the eBay search URL for the given search keyword.

    Args:
        search_keyword (str): The search keyword.

    Returns:
        str: The constructed eBay search URL.
    """
    # Base URL for eBay search
    base_url = "https://www.ebay.com/sch/i.html"

    if not search_keyword:
        raise ValueError("Keyword must be provided for the search.")

    # Query parameters
    params = {
        "_nkw": search_keyword.replace(" ", "+"),
        "_sacat": "0",
        "_ipg": "120",
        "LH_BIN": "1",
        "LH_ItemCondition": "3",
        "LH_PrefLoc": "1",
        # "Brand": "Lexus|OEM|Toyota",
        "Brand%20Type": "Genuine%20OEM",
    }

    # Construct the query string
    query_string = urlencode(params, doseq=True)

    # Construct the full URL using urljoin
    return urljoin(base_url, f"?{query_string}")


def build_seller_search_url(seller_id: str) -> str:
    """
    Build an eBay seller search URL based on the seller ID.

    Args:
        seller_id (str): The seller's username or ID.

    Returns:
        str: The complete eBay seller search URL.
    """
    base_url = "https://www.ebay.com/sch/i.html"

    if not seller_id:
        raise ValueError("Seller ID must be provided for the search.")

    # Define the query parameters
    params = {
        "_fss": "1",
        "_saslop": "1",
        "_sasl": seller_id,
        "LH_SpecificSeller": "1",
        "_dmd": "1",
    }

    # Construct the query string
    query_string = urlencode(params)

    # Construct the full URL using urljoin
    return urljoin(base_url, f"?{query_string}")


def build_tosshin_url(keyword: str) -> str:
    """
    Build the URL for the Tosshin search page with the given keyword.

    Args:
        keyword (str): The search keyword.

    Returns:
        str: The constructed Tosshin URL.
    """
    # Base URL for Tosshin search
    base_url = "https://tosshin.com/en/search.html"

    if not keyword:
        raise ValueError("Keyword must be provided for the search.")

    # Query parameters
    params = {
        "partNo": keyword,
    }

    # Construct the query string
    query_string = urlencode(params)

    # Construct the full URL using urljoin
    return urljoin(base_url, f"?{query_string}")


def build_apec_manufacturer_search(keyword: str) -> str:
    """
    Build the URL for the APEC Auto search page with the given keyword.

    Args:
        keyword (str): The search keyword.

    Returns:
        str: The constructed APEC Auto URL.
    """
    # Base URL for APEC Auto search
    base_url = "https://apecauto.com/searchmanufacturers.aspx/"

    if not keyword:
        raise ValueError("Keyword must be provided for the search.")

    # Query parameters
    params = {
        "pn": keyword,
        "st": 1,
    }

    # Construct the query string
    query_string = urlencode(params)

    # Construct the full URL using urljoin
    return urljoin(base_url, f"?{query_string}")


def escape_quotes(text: Optional[str]) -> Optional[str]:
    """
    Escapes quotation marks in the given text to ensure proper Excel formula formatting.

    Args:
        text (Optional[str]): The text in which to escape quotation marks.

    Returns:
        Optional[str]: The text with escaped quotation marks.
    """
    if text is None:
        return None
    return text.replace('"', '""')


def ebay_clean_product_url(url: Optional[str]) -> Optional[str]:
    """
    Trims everything after the question mark in a URL, including the question mark.

    Args:
        url (Optional[str]): The original URL to be cleaned.

    Returns:
        Optional[str]: The cleaned URL with query parameters removed.
    """
    if url is None:
        return None
    return url.split("?")[0]


def convert_to_excel_date(date: Optional[datetime]) -> Optional[float]:
    """
    Convert a datetime object to an Excel serial date number.

    Args:
        date (Optional[datetime]): The datetime object to convert.

    Returns:
        Optional[float]: Excel serial date number.
    """
    if date is None:
        return None
    temp = datetime(1899, 12, 30)  # Excel's epoch
    delta = date - temp
    return float(delta.days) + (float(delta.seconds) / 86400)


def calculate_ebay_dates(
    day_range: int, end_date: Optional[datetime] = None
) -> tuple[int, int]:
    """
    Calculate the start date and end date in Unix timestamp format (milliseconds).

    Args:
        day_range (int): Number of days for the search range (e.g., 30 or 90).
        end_date (Optional[datetime]): The end date for the range (defaults to current date/time).

    Returns:
        tuple[int, int]: A tuple containing the startDate and endDate in milliseconds.
    """
    if end_date is None:
        end_date = datetime.now()

    # Calculate the start date
    start_date = end_date - timedelta(days=day_range)

    # Convert to Unix timestamps in milliseconds
    start_timestamp = int(start_date.timestamp() * 1000)
    end_timestamp = int(end_date.timestamp() * 1000)

    return start_timestamp, end_timestamp


def handle_scraping_exception(e: Exception, search_keyword: str) -> None:
    """
    Handle exceptions that occur during scraping.

    Args:
        e (Exception): The exception that occurred.
        search_keyword (str): The search keyword for which the exception occurred.
    """
    if isinstance(e, TimeoutException):
        logging.error("Timeout occurred while fetching data for '%s'.", search_keyword)
    elif isinstance(e, NoSuchElementException):
        logging.error(
            "Element not found while fetching data for '%s': %s", search_keyword, e
        )
    else:
        logging.error(
            "An error occurred while fetching data for '%s': %s", search_keyword, e
        )


def get_enum_header(enum_class: Enum) -> str:
    """
    Retrieves the header string from an enum class if it exists.

    Checks if the enum value is a dictionary and if it contains the header key.
    Returns the header string or "Error" if not found.
    """
    if not isinstance(enum_class.value, DataAttr):
        return "Error"
    header = enum_class.value.header
    return header or "Error"


def get_enum_col(enum_class: Enum) -> int:
    """
    Retrieves the column index from an enum class if it exists.

    Checks if the enum value is a dictionary and if it contains the column key.
    Returns the column index or 0 if not found.
    """
    if not isinstance(enum_class.value, DataAttr):
        return 0
    return enum_class.value.column or 0


def get_enum_last_col(enum_class) -> int:
    """
    Finds the largest column index across all members of the enum class.

    Returns the maximum column index or 0 if no column indices are found.
    """
    max_col = 0
    for member in enum_class:
        # Ensure the value is a DataAttr instance
        if isinstance(member.value, DataAttr):
            max_col = max(max_col, member.value.column or 0)
    return max_col


def get_enum_headers_row(enum_class: Type[Enum]) -> list[str]:
    """
    Retrieves and sorts headers from an enum class based on their column indices.

    Returns a list of headers in the order of their column indices.
    """
    headers_with_col = [
        (data.value.header, data.value.column)
        for data in enum_class
        if isinstance(data.value, DataAttr)
    ]
    sorted_headers = sorted(headers_with_col, key=lambda x: x[1] or 0)
    return [header for header, _ in sorted_headers if header is not None]


def write_data(
    worksheet: xlsxwriter.worksheet.Worksheet,
    formats: dict[FormatType, xlsxwriter.format.Format],
    row: int,
    col: int,
    data: dict,
    data_key: Enum,
    url_key: Optional[Enum] = None,
    url_string: str = "",
    check_genuine: bool = False,
    is_date: bool = False,
    is_currency: bool = False,
    lock: Optional[Lock] = None,
):
    """
    Writes data to an Excel worksheet cell with appropriate formatting.

    Handles URL writing, number formatting (date, currency, float), and string writing.
    Applies special formatting based on the value's characteristics and provided conditions.

    If a lock is provided, it will be acquired before writing to ensure thread safety.
    """

    def _write_to_worksheet(
        worksheet: xlsxwriter.worksheet.Worksheet,
        formats: dict[FormatType, xlsxwriter.format.Format],
        row: int,
        col: int,
        data: dict,
        data_key: Enum,
        url_key: Optional[Enum] = None,
        url_string: str = "",
        check_genuine: bool = False,
        is_date: bool = False,
        is_currency: bool = False,
    ):
        """
        Private helper function to handle writing data to a worksheet without lock handling.
        """
        # Extract value and determine format
        value = data.get(data_key, "")
        cell_format = None

        if check_genuine and isinstance(value, str) and "genuine" in value.lower():
            cell_format = (
                formats[FormatType.FILL_URL] if url_key else formats[FormatType.FILL]
            )

        # Determine if URL key exists and is not None
        has_valid_url_key = url_key is not None and data.get(url_key) is not None

        # Determine cell format based on conditions
        format_conditions = {
            "url": has_valid_url_key,
            "is_date": is_date,
            "is_currency": is_currency,
            "is_float": isinstance(value, float),
            "is_number": isinstance(value, (int, float)),
        }

        if format_conditions["url"]:
            worksheet.write_url(
                row,
                col,
                data[url_key],
                string=value or url_string,
                cell_format=cell_format,
            )
        elif format_conditions["is_number"]:
            if format_conditions["is_date"]:
                cell_format = formats.get(FormatType.DATE)
            elif format_conditions["is_currency"]:
                cell_format = formats.get(FormatType.CURRENCY)
            elif format_conditions["is_float"]:
                cell_format = formats.get(FormatType.FLOAT)
            else:
                cell_format = formats.get(FormatType.NUMBER)

            worksheet.write_number(row, col, value, cell_format=cell_format)
        else:
            worksheet.write_string(
                row, col, str(value) if value else "", cell_format=cell_format
            )

    if lock:
        with lock:
            _write_to_worksheet(
                worksheet,
                formats,
                row,
                col,
                data,
                data_key,
                url_key,
                url_string,
                check_genuine,
                is_date,
                is_currency,
            )
    else:
        _write_to_worksheet(
            worksheet,
            formats,
            row,
            col,
            data,
            data_key,
            url_key,
            url_string,
            check_genuine,
            is_date,
            is_currency,
        )


def take_screenshot(
    filepath: str, driver: webdriver.Chrome, ss_lock: Optional[Lock] = None
) -> bool:

    def _take_screenshot(filepath: str, driver: webdriver.Chrome) -> bool:
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            original_size = driver.get_window_size()
            required_width = driver.execute_script(
                "return document.body.parentNode.scrollWidth"
            )
            required_height = driver.execute_script(
                "return document.body.parentNode.scrollHeight"
            )
            driver.set_window_size(required_width, required_height)
            ss_success = driver.find_element(By.TAG_NAME, "body").screenshot(
                filepath
            )  # avoids scrollbar
            driver.set_window_size(original_size["width"], original_size["height"])
            if ss_success:
                logging.info(f"Screenshot saved at {filepath}")
                return True
            else:
                logging.error("Failed to save screenshot.")
                return False
        except TimeoutException:
            logging.error("Timed out waiting for body element to be present")
            return False

    if ss_lock:
        with ss_lock:
            return _take_screenshot(filepath, driver)
    else:
        return _take_screenshot(filepath, driver)


def add_screenshot_to_sheet(
    sheet: xlsxwriter.worksheet.Worksheet, row: int, file_path: str
) -> None:
    try:
        # Attempt to insert the image into the specified sheet and row
        sheet.insert_image(row, 0, file_path)
        logging.info(f"Inserted screenshot from {file_path} at row {row}")
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
    except Exception as e:
        logging.error(f"Error inserting image at row {row}: {e}")
