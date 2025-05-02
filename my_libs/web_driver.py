import json
import logging
import os
import time
from queue import Queue
from threading import Lock

from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    WebDriverException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

os.environ["WDM_LOG"] = str(logging.NOTSET)
cookie_update_lock = Lock()


class DriverPool:
    def __init__(self, max_workers: int) -> None:
        logging.info(f"Initializing driver pool with {max_workers} drivers...")
        self.pool = Queue(max_workers)
        for _ in range(max_workers):
            driver = initialize_driver()
            self.pool.put(driver)

    def acquire(self) -> webdriver.Chrome:
        return self.pool.get()

    def release(self, driver) -> None:
        self.pool.put(driver)

    def cleanup(self) -> None:
        while not self.pool.empty():
            driver = self.pool.get()
            close_driver(driver)


def initialize_driver(headless: bool = True) -> webdriver.Chrome:
    """
    Initializes the Chrome WebDriver with specified options.

    Args:
        headless (Optional[bool]): Whether to run Chrome in headless mode. Default is True.

    Returns:
        webdriver.Chrome: Configured Chrome WebDriver instance.
    """
    logging.debug(
        "Initializing Chrome WebDriver with headless mode set to %s.", headless
    )
    chrome_options = Options()
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.96 Safari/537.36"
    )
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-logging")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--incognito")
    try:
        driver = webdriver.Chrome(
            service=ChromeService(ChromeDriverManager().install()),
            options=chrome_options,
        )
        logging.info(
            f"Chrome WebDriver initialized successfully with headless mode set to {headless}."
        )
    except Exception as e:
        logging.error(f"Failed to initialize driver: {e}", exc_info=True)
        raise

    return driver


def close_driver(driver: webdriver.Chrome) -> None:
    logging.info("Closing WebDriver...")
    try:
        driver.quit()
        logging.info("WebDriver closed.")
    except Exception as e:
        logging.error(f"Error raised when closing WebDriver: {e}")


def handle_ebay_session(
    driver: webdriver.Chrome, use_fresh_session: bool = False
) -> bool:
    """
    Manages the loading and saving of cookies for the WebDriver session.

    This function attempts to load cookies from a file. If loading cookies fails
    (e.g., due to invalid or missing cookies), it prompts the user to log in manually,
    saves the new cookies, and then reattempts to load the new cookies.

    Args:
        driver (WebDriver): The Chrome WebDriver instance to use.
        use_fresh_session (bool): Whether to skip loading existing cookies and force a new login. Defaults to False.

    Returns:
        bool: True if cookies were successfully loaded/saved, False if all attempts failed.
    """
    cookies_file = "cookies.json"
    max_retries = 5

    logging.debug(
        "Starting ebay_handle_cookies with use_fresh_session_file=%s", use_fresh_session
    )

    if not use_fresh_session and ebay_load_and_apply_cookies(driver, cookies_file):
        return True

    logging.warning("Cookies invalid or missing, attempting to obtain new cookies")
    for attempt in range(1, max_retries + 1):
        logging.info(f"Attempt to handle cookies... ({attempt} / {max_retries})")

        with cookie_update_lock:
            logging.debug("Acquired cookie_update_lock for cookie management.")
            # Check if cookies have already been updated by another instance
            if ebay_load_and_apply_cookies(
                driver, cookies_file
            ) and verify_cookies_bypass_captcha(driver):
                logging.info(
                    "Cookies have been successfully updated by another instance. Proceeding with the current session."
                )
                return True

            # Attempt to initialize a visible driver for user login if cookies are still invalid
            logging.debug(
                "Initializing a visible login driver for user authentication."
            )
            try:
                login_driver = initialize_driver(headless=False)
                logging.debug("Visible login driver initialized successfully.")
            except Exception as e:
                logging.error("Error initializing visible login driver: %s", str(e))
                continue

            try:
                logging.debug("Prompting user for login credentials.")
                if ebay_prompt_user_login(login_driver, cookies_file):
                    logging.debug(
                        "User login successful; applying cookies to the main driver."
                    )
                    # Load and apply cookies using the original headless driver
                    if ebay_load_and_apply_cookies(
                        driver, cookies_file
                    ) and verify_cookies_bypass_captcha(driver):
                        return True
                else:
                    logging.error("Login failed; unable to save cookies. Retrying...")

            finally:
                # Ensure the login driver is closed properly
                close_driver(login_driver)

    logging.error("Max retries reached. Cookie handling failed.")
    return False


def ebay_load_and_apply_cookies(driver: webdriver.Chrome, cookies_file: str) -> bool:
    """
    Tries to load cookies from a file and applies them to the WebDriver session.

    Args:
        driver (webdriver.Chrome): The Chrome WebDriver instance to use.
        cookies_file (str): The path to the cookies file.

    Returns:
        bool: True if cookies were successfully loaded and applied, False otherwise.
    """
    logging.debug("Attempting to load and apply cookies from %s", cookies_file)
    try:
        logging.debug("Loading cookies from file")
        load_ebay_cookies(driver, cookies_file)

        logging.debug("Refreshing page to apply cookies")
        driver.refresh()  # Refresh to apply cookies

        logging.debug("Waiting for page body to load")
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
        )
        logging.info("Cookies loaded successfully and the page has been refreshed.")
        return True
    except Exception as e:
        logging.error("Failed to load cookies: %s", e)
        return False


def ebay_prompt_user_login(login_driver: webdriver.Chrome, cookies_file: str) -> bool:
    """
    Prompts the user to log in manually and saves the new cookies.

    Args:
        driver (webdriver.Chrome): The Chrome WebDriver instance to use.
        cookies_file (str): The path to the cookies file.

    Returns:
        bool: True if login and saving cookies were successful, False otherwise.
    """

    def read_credentials(credentials_file: str) -> tuple:
        """
        Reads email and password from the provided credentials file.

        Args:
            credentials_file (str): The path to the credentials file.

        Returns:
            tuple: A tuple containing email and password. If the file or credentials are missing, returns (None, None).
        """
        email = None
        password = None

        if os.path.exists(credentials_file):
            try:
                with open(credentials_file, "r") as file:
                    lines = file.readlines()

                    for line in lines:
                        if line.startswith("Email:"):
                            email = line.split("Email:")[1].strip()
                        elif line.startswith("Password:"):
                            password = line.split("Password:")[1].strip()

                    # Logging for feedback
                    if email or password:
                        logging.info("Credentials loaded successfully.")
                    else:
                        logging.warning("Credentials file is empty.")
            except Exception as e:
                logging.error(f"Error reading credentials file: {e}")
        else:
            logging.warning(f"Credentials file '{credentials_file}' not found.")

        return email, password

    logging.warning(
        "Cookies are invalid or missing. User will need to log in manually."
    )
    login_url = "https://www.ebay.com/signin/"
    login_driver.get(login_url)

    # email, password = read_credentials("eBayLogin.txt")
    # Monitor the browser window during the login process
    login_driver = monitor_browser(login_driver, login_url)

    # Save new cookies after user login
    try:
        save_cookies(login_driver, cookies_file)
        return True
    except Exception:
        return False


def ebay_wait_for_user_login(driver: webdriver.Chrome) -> None:
    """
    Prompts the user to complete the login process manually.

    This function waits until the user has logged in by checking for the presence
    of a specific element that indicates a successful login.

    Args:
        driver (webdriver.Chrome): The Chrome WebDriver instance to use.
    """
    logging.warning(
        "Please complete the login process manually, including solving the reCAPTCHA if required."
    )
    while True:
        try:
            # Check for the logged-in user element
            logged_in_element = driver.find_element(By.ID, "gh-ug")
            logged_in_element_classes = logged_in_element.get_attribute("class")
            # Check if the element has the 'gh-control' class
            if logged_in_element_classes and "gh-control" in logged_in_element_classes:
                logging.info("User login detected, proceeding with the session.")
                break
            else:
                logging.debug("User is not logged in yet.")
        except Exception:
            logging.debug("Waiting for user login to complete...")
            time.sleep(5)  # Check every 5 seconds


def save_cookies(driver: webdriver.Chrome, filename: str) -> None:
    """
    Saves the current session cookies to a file.

    Args:
        driver (WebDriver): The Chrome WebDriver instance to use.
        filename (str): The path to the file where cookies will be saved.
    """
    with open(filename, "w") as file:
        cookies = driver.get_cookies()
        json.dump(cookies, file)
    logging.info("Cookies successfully saved to '%s'.", filename)


def load_ebay_cookies(driver: webdriver.Chrome, filename: str) -> None:
    """
    Loads cookies from a file and adds them to the WebDriver session.

    Args:
        driver (WebDriver): The Chrome WebDriver instance to use.
        filename (str): The path to the file from which cookies will be loaded.

    Raises:
        FileNotFoundError: If the specified cookie file does not exist.
        RuntimeError: If there is an issue loading cookies from the file or adding them to the driver.
    """
    if not os.path.isfile(filename):
        message = f"File '{filename}' does not exist."
        raise FileNotFoundError(message)

    try:
        with open(filename, "r") as file:
            cookies = json.load(file)

        driver.get("https://www.ebay.com")
        for cookie in cookies:
            try:
                if "domain" in cookie and cookie["domain"] != ".ebay.com":
                    logging.debug("Skipping cookie with domain '%s'.", cookie["domain"])
                    continue
                driver.add_cookie(cookie)
            except Exception as e:
                logging.warning("Error adding cookie '%s': %s", cookie, e)

        logging.info("Cookies loaded and added to WebDriver session.")

    except json.JSONDecodeError as e:
        message = f"Error decoding cookies from file '{filename}': {e}"
        raise RuntimeError(message)
    except Exception as e:
        message = f"Failed to load cookies from '{filename}': {e}"
        raise RuntimeError(message)


def monitor_browser(
    login_driver: webdriver.Chrome,
    destination: str,
    # email: Optional[str] = None,
    # password: Optional[str] = None,
) -> webdriver.Chrome:
    """
    Monitors the browser window to ensure it remains open during the login process.
    If the browser is closed, it reinitializes the WebDriver and reloads the login page.

    Args:
        driver (WebDriver): The Chrome WebDriver instance to monitor.
        login_url (str): The URL to reload if the browser is closed.
        email (Optional[str]): The email address for auto-fill
        password (Optional[str]): The password for auto-fill

    Returns:
        WebDriver: A WebDriver instance that is actively monitoring the browser window.
    """
    while True:
        try:
            # Check if browser is still open by accessing the current URL
            login_driver.current_url
            # if email:
            #     try:
            #         email_input = current_driver.find_element(By.ID, "userid")
            #         current_email_value = email_input.get_attribute("value")
            #         if current_email_value != email:
            #             email_input.clear()
            #             email_input.send_keys(email)
            #             logging.info("Email field filled.")
            #     except NoSuchElementException:
            #         pass
            # if password:
            #     try:
            #         password_input = current_driver.find_element(By.ID, "pass")
            #         current_password_value = password_input.get_attribute("value")
            #         if current_password_value != password:
            #             password_input.clear()
            #             password_input.send_keys(password)
            #             logging.info("Password field filled.")
            #     except NoSuchElementException:
            #         pass
        except WebDriverException:
            logging.error("Browser was closed accidentally. Reinitializing...")
            close_driver(login_driver)
            login_driver = initialize_driver(headless=False)
            login_driver.get(destination)
            continue

        # Check if user has completed login (customize this check as needed)
        if "signin" not in login_driver.current_url:
            break
        else:
            logging.info("Waiting for user to sign in...")

        time.sleep(5)  # Wait before checking again

    return login_driver


def reload_ebay_cookies(
    driver: webdriver.Chrome, destination: str, use_fresh_session: bool = False
) -> None:
    """
    Quits the existing driver and initializes a new headless driver.

    :param driver: The Selenium WebDriver instance.
    :param destination: The URL to load after CAPTCHA or login is handled.
    :param use_fresh_session: Whether to skip loading cookies from file.
    """
    logging.debug("Deleting all cookies")
    driver.delete_all_cookies()
    logging.debug(f"Handling cookies with use_fresh_session={use_fresh_session}")
    if not handle_ebay_session(driver, use_fresh_session):
        raise Exception("Failed to handle cookies.")
    logging.debug(f"Navigating to destination: {destination}")
    driver.get(destination)  # Navigate to the destination


def attempt_captcha_bypass(driver: webdriver.Chrome, destination: str) -> bool:
    """
    Attempts to bypass CAPTCHA by reloading cookies multiple times.

    Args:
        driver: The Selenium WebDriver instance.
        destination: The URL to load after CAPTCHA or login is handled.

    Returns:
        bool: True if bypass was successful, False if max attempts reached
    """
    max_attempts: int = 5
    attempts: int = 0

    while attempts < max_attempts:
        logging.warning(
            f"CAPTCHA or Login detected. Attempting to bypass by reloading cookies... ({attempts+1} / {max_attempts})"
        )
        reload_ebay_cookies(driver, destination)
        attempts += 1

        if verify_cookies_bypass_captcha(driver):
            logging.info("CAPTCHA Bypassed successfully.")
            return True

    logging.warning("CAPTCHA Bypass failed after maximum attempts.")
    return False


def check_ebay_captcha(driver: webdriver.Chrome, destination: str) -> None:
    """
    Pauses the script and waits for the user to solve the CAPTCHA and handle login.

    Args:
        driver: The Selenium WebDriver instance.
        destination: The URL to load after CAPTCHA or login is handled.
    """
    attemped_bypass = False

    logging.debug("Starting CAPTCHA/login check loop")
    while True:
        logging.debug(f"Current URL: {driver.current_url}")

        # Handle CAPTCHA/Login pages
        if (
            "www.ebay.com/splashui/captcha" in driver.current_url
            or "signin.ebay.com" in driver.current_url
        ):

            if not attemped_bypass:
                attemped_bypass = True
                if attempt_captcha_bypass(driver, destination):
                    continue

            # If bypass failed, try manual login
            logging.warning(
                "Maximum attempts to bypass reached. Proceeding with manual login..."
            )
            logging.info("Proceeding with manual login using non-headless Chrome.")
            reload_ebay_cookies(driver, destination, use_fresh_session=True)
            continue

        # Handle passkey registration
        elif "accounts.ebay.com/acctsec/authn-register" in driver.current_url:
            logging.debug("Passkey registration page detected")
            try:
                skip_button = driver.find_element(By.ID, "passkeys-cancel-btn")
                logging.debug("Found 'Skip for now' button, clicking...")
                skip_button.click()
                logging.info("Clicked 'Skip for now' on passkey registration page.")
                time.sleep(2)
            except NoSuchElementException:
                logging.warning(
                    "'Skip for now' button not found on passkey page. Please handle manually."
                )

        # Handle limit exceeded
        elif driver.current_url == "https://pages.ebay.com/limitexceeded.html":
            logging.error("Limit exceeded page detected. Stopping scraper.")
            raise Exception("LimitExceededException")

        # No issues detected
        else:
            logging.debug("No CAPTCHA/login/passkey/limit issues detected")
            break

        logging.debug("Waiting 5 seconds before next check")
        time.sleep(5)


def verify_cookies_bypass_captcha(driver: webdriver.Chrome) -> bool:
    """
    Verify if the current cookies successfully bypass CAPTCHA/login pages.

    Args:
        driver: The Selenium WebDriver instance.

    Returns:
        bool: True if no CAPTCHA/login page is detected, False otherwise.
    """
    return not (
        "www.ebay.com/splashui/captcha" in driver.current_url
        or "signin.ebay.com" in driver.current_url
    )


def save_html(driver: webdriver.Chrome, filename: str) -> None:
    with open(f"{filename}.html", "w", encoding="utf-8") as f:
        f.write(driver.page_source)
