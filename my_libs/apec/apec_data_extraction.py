import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Any, Callable
from urllib.parse import parse_qs, urljoin, urlparse

import my_libs.utils as Utils
import my_libs.web_driver as Driver
from my_libs.apec.apec_xlsx_writer import ApecData, MyApecExcel
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def process_keywords(keywords: list[str], output_dir: str) -> None:
    """
    Fetches manufacturer URLs for given keywords and processes data scraping concurrently.

    Args:
        keywords (list[str]): List of keywords to search for manufacturers.
        output_dir (str): Directory path to save the scraped data.
    """
    if not keywords:
        logging.warning("No keywords provided. Skipping data fetch.")
        return

    logging.info("Fetching and saving data for keywords: %s", ", ".join(keywords))

    # Fetch all manufacturer URLs
    manufacturer_urls_dict = fetch_all_manufacturer_urls(keywords)

    if manufacturer_urls_dict:
        # Process and scrape the data
        process_and_scrape_manufacturer_data(manufacturer_urls_dict, output_dir)


def fetch_all_manufacturer_urls(keywords: list[str]) -> dict[str, list[str]]:
    """
    Fetches manufacturer URLs for all provided keywords.

    Args:
        keywords (list[str]): List of keywords to search manufacturers for.

    Returns:
        dict[str, list[str]]: Dictionary mapping keywords to manufacturer URLs.
    """
    manufacturer_urls_dict: dict[str, list[str]] = {}
    driver: webdriver.Chrome = Driver.initialize_driver()

    try:
        for keyword in keywords:
            keyword = keyword.strip()
            if keyword:
                manufacturer_urls = get_manufacturer_urls(driver, keyword)
                manufacturer_urls_dict[keyword] = manufacturer_urls
            else:
                logging.warning("Empty search keyword encountered. Skipping...")
    except Exception as e:
        Utils.handle_scraping_exception(e, "Apec Scraper")
    finally:
        driver.quit()  # Quit driver after fetching manufacturer URLs

    return manufacturer_urls_dict


def process_and_scrape_manufacturer_data(
    manufacturer_urls_dict: dict[str, list[str]], output_dir: str
) -> None:
    """
    Processes the manufacturer data by scraping it using multiple threads.

    Args:
        manufacturer_urls_dict (dict): A dictionary of keywords and corresponding manufacturer URLs.
        output_dir (str): Directory path to save the scraped data.
    """
    workbook: MyApecExcel = MyApecExcel("APEC Auto Data", output_dir)
    screenshot_folder_path = Utils.create_subfolder(output_dir, "APEC Screenshots")
    all_tasks = []
    excel_writing_lock = Lock()
    ss_lock = Lock()
    max_workers = 3
    driver_pool = Driver.DriverPool(max_workers)
    try:
        # Use thread pool to scrape manufacturer data concurrently
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for keyword, manufacturer_urls in manufacturer_urls_dict.items():
                for url in manufacturer_urls:
                    # Submit a task for each manufacturer to scrape their data concurrently
                    task = executor.submit(
                        scrape_manufacturer_data,
                        url,
                        workbook,
                        keyword,
                        screenshot_folder_path,
                        driver_pool,
                        excel_writing_lock,
                        ss_lock,
                    )
                    all_tasks.append(task)

            # Wait for all tasks to complete
            for task in as_completed(all_tasks):
                try:
                    task.result()
                except Exception as e:
                    logging.error(f"Task raised an exception: {e}")
        # for idx, filepath in enumerate(SCREENSHOTS_LIST):
        #     workbook.add_screenshot(idx, filepath)
        workbook.save_workbook()
        logging.info("All tasks completed successfully")

    except Exception as e:
        Utils.handle_scraping_exception(e, "Apec Scraper")
    finally:
        driver_pool.cleanup()


def get_manufacturer_urls(driver: webdriver.Chrome, keyword: str) -> list[str]:
    """
    Extracts the manufacturer names and links from the current page for the given keyword.

    Args:
        driver (webdriver.Chrome): The Selenium WebDriver instance.
        keyword (str): The keyword to search.

    Returns:
        list[str]: A list of manufacturer page URLs.
    """
    manufacturer_urls: list[str] = []
    initial_url = Utils.build_apec_manufacturer_search(keyword)
    try:
        logging.info(
            f"Searching for manufacturers for keyword '{keyword}', navigating to {initial_url}..."
        )
        driver.get(initial_url)

        # Wait for the rows to load, with a timeout of 10 seconds
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.table__rows a"))
        )

        # Check for the error message indicating no results
        try:
            error_message = driver.find_element(
                By.ID, "ctl00__content_SparePartsManufacturers1__errorLabel"
            )
            if error_message and error_message.text == "None":
                logging.warning(f"No results found for keyword '{keyword}'.")
                return manufacturer_urls  # Return empty list if no results
        except NoSuchElementException:
            logging.debug(
                f"No error message found for keyword '{keyword}'. Proceeding with extraction."
            )

        current_url = driver.current_url

        if initial_url != current_url and "searchspareparts" in current_url:
            logging.info(f"Redirected to manufacturer page: {current_url}")
            manufacturer_urls.append(current_url)
        else:
            rows = driver.find_elements(By.CSS_SELECTOR, "div.table__rows a")
            for row in rows:
                # Extract the link
                row_link = row.get_attribute("href")
                full_link = urljoin("https://apecauto.com/", row_link)

                manufacturer_urls.append(full_link)
                logging.debug(f"Found manufacturer link: {full_link}")

    except Exception as e:
        logging.error(f"Error extracting manufacturer links for keyword {keyword}: {e}")

    return manufacturer_urls


def scrape_manufacturer_data(
    url: str,
    workbook: MyApecExcel,
    keyword: str,
    screenshot_folder_path: str,
    driver_pool: Driver.DriverPool,
    excel_writing_lock: Lock,
    ss_lock: Lock,
) -> None:
    """
    Scrapes manufacturer data from the given URL and writes it to the provided workbook.

    Args:
        url (str): The URL of the manufacturer's page.
        workbook (MyApecExcel): The Excel workbook to write data to.
        keyword (str): The keyword used for searching manufacturers.
        screenshot_folder_path (str): The path to the image folder where the screenshot will be saved
        lock (Lock): The threading lock to prevent conflict data writing
    """
    driver = driver_pool.acquire()
    page_num = 1
    try:
        # Navigate to the APEC page initially
        logging.info(f"Navigating to APEC page: {url}")
        driver.get(url)

        # Ensure the page is loaded before scraping
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.table__rows-group"))
        )

        should_continue = True  # Flag to control the scraping loop
        logging.info(f"Scraping data from {driver.current_url}")
        while should_continue:
            # Scrape data
            groups = driver.find_elements(By.CSS_SELECTOR, "div.table__rows-group")
            logging.info(f"Found {len(groups)} groups on the page.")

            current_url = driver.current_url
            parsed_url = urlparse(current_url)
            query_params = parse_qs(parsed_url.query)
            mfr_value = query_params.get("mfr", ["unknown"])[0]

            with ss_lock:
                screenshot_path = os.path.join(
                    screenshot_folder_path,
                    f"{keyword} {mfr_value} page {page_num}.png",
                )
                if Utils.take_screenshot(screenshot_path, driver):
                    page_num += 1

            for group in groups:
                try:
                    category_name = group.find_element(
                        By.CSS_SELECTOR, "div.table__rows-title"
                    ).text
                    logging.info(f"Processing {keyword}'s category: {category_name}")

                    if category_name not in [
                        "Own stock warehouses",
                        "Requested article",
                        "Superseded part for the requested article",
                    ]:
                        logging.warning(
                            f"Aborting scraping for category: {category_name}. Only specific categories are needed."
                        )
                        should_continue = False  # Set flag to stop scraping
                        break  # Exit the inner loop

                    rows = group.find_elements(
                        By.CSS_SELECTOR, "div.table__rows-list > div.table__row"
                    )
                    logging.info(
                        f"Found {len(rows)} rows in {keyword}'s category '{category_name}'."
                    )

                    for row in rows:
                        try:
                            data = parse_row_data(row, category_name, keyword)
                            workbook.write_data_row(data, excel_writing_lock)
                        except Exception as e:
                            logging.error(
                                f"Error parsing row in {keyword}'s category '{category_name}': {e}"
                            )
                except Exception as e:
                    logging.error(
                        f"Error processing group in {keyword}'s category '{category_name}': {e}"
                    )

            # Check if should continue before navigating to the next page
            if should_continue:
                # Check if there is a next page
                try:
                    next_button = driver.find_element(By.CSS_SELECTOR, "li.page-next")
                    class_attr = next_button.get_attribute("class")
                    if class_attr and "disabled" in class_attr:
                        logging.info("No more pages to scrape.")
                        should_continue = False  # Set flag to stop scraping
                    else:
                        driver.execute_script(
                            "window.scrollTo(0, document.body.scrollHeight);"
                        )
                        next_button.click()
                        # Wait for the new groups to load
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located(
                                (By.CSS_SELECTOR, "div.table__rows-group")
                            )
                        )
                except NoSuchElementException:
                    logging.info("Next page button not found. Ending pagination.")
                    should_continue = False  # Set flag to stop scraping
                except Exception as e:
                    logging.error(f"Error clicking next page button: {e}")
                    should_continue = False  # Set flag to stop scraping

    except Exception as e:
        logging.error(f"Error scraping data from {url}: {e}")

    finally:
        driver_pool.release(driver)


def parse_row_data(row: WebElement, category: str, keyword: str) -> dict[ApecData, Any]:
    """
    Parses data from a row element and returns it as a dictionary.

    Args:
        category (str): The category of the row data.
        row (WebElement): The Selenium WebElement representing the row.

    Returns:
        dict[ApecData, Any]: Dictionary containing parsed row data.
    """

    def safe_extract_text(selector, transform_func: Callable[[str], Any] = str) -> Any:
        """
        Safely extract text from an element located by the CSS selector and transform it using a function.

        Args:
            selector: The CSS selector to locate the element.
            transform_func: Function to transform the extracted text.

        Returns:
            Any: Transformed text or None if extraction fails.
        """
        try:
            element = row.find_element(By.CSS_SELECTOR, selector)
            text = element.text.strip()
            # Check if the element has an <i> tag with a title attribute
            i_elements = element.find_elements(By.TAG_NAME, "i")
            if i_elements:
                i_element = i_elements[0]
                class_attr = i_element.get_attribute("class")
                if class_attr and "icon-nal" in class_attr:
                    return "✔"
            transformed_text = transform_func(text)
            if transformed_text is None or transformed_text == "":
                return text
            return transformed_text
        except (NoSuchElementException, ValueError):
            logging.debug("Failed to extract data for '%s'", selector)
            return None

    data: dict[ApecData, Any] = {}

    # Define lambda functions with added robustness
    weight_transform = lambda text: (
        float(text) if text and text.replace(".", "", 1).isdigit() else None
    )
    availability_transform = lambda text: int(text) if text.isdigit() else None
    lead_time_transform = lambda text: int(text) if text.isdigit() else None
    price_transform = lambda text: (
        float(text.replace(",", "").replace("$", ""))
        if text and text.replace(".", "", 1).isdigit()
        else None
    )
    data[ApecData.KEYWORD] = keyword
    data[ApecData.CATEGORY] = category
    data[ApecData.MANUFACTURER] = safe_extract_text(
        "div.table__row-element:nth-of-type(1) > div"
    )
    data[ApecData.ARTICLE] = safe_extract_text(
        "div.table__row-element:nth-of-type(2) > div"
    )

    # Extract and handle NAME column
    name_element = row.find_element(
        By.CSS_SELECTOR, "div.table__row-element:nth-of-type(3) > div"
    )
    hyperlinks = name_element.find_elements(By.TAG_NAME, "a")

    if hyperlinks:
        hyperlink = hyperlinks[0]  # Assuming there's only one hyperlink
        data[ApecData.NAME] = hyperlink.text
        data[ApecData.NAME_URL] = hyperlink.get_attribute("href")
    else:
        data[ApecData.NAME] = name_element.text.strip()

    data[ApecData.WEIGHT] = safe_extract_text(
        "div.table__row-element:nth-of-type(4) > div", weight_transform
    )
    data[ApecData.AVAILABILITY] = safe_extract_text(
        "div.table__row-element:nth-of-type(5) > div", availability_transform
    )
    data[ApecData.LEAD_TIME] = safe_extract_text(
        "div.table__row-element:nth-of-type(6) > div", lead_time_transform
    )
    data[ApecData.INFORMATION] = safe_extract_text(
        "div.table__row-element:nth-of-type(7) > div"
    )
    data[ApecData.PRICE] = safe_extract_text(
        "div.table__row-element:nth-of-type(8) > div", price_transform
    )
    return data
