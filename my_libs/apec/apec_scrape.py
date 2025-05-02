from my_libs.apec.apec_data_extraction import process_keywords
from my_libs.dependencies import *


def scrape(keywords: list[str], output_folder: str) -> None:
    """
    Scrape function to execute the web scraping process.

    This function sets up logging, initializes the WebDriver, handles cookies,
    creates a new workbook, fetches and saves product data based on provided
    keywords, and saves the workbook to the specified output folder.

    Args:
        keywords (list of str): List of search keywords to scrape data for.
        output_folder (str): Path to the folder where the output files will be saved.
    """
    # Configure logging
    start_time = perf_counter()
    logging.info("Starting the eBay Listings scrape execution...")

    try:

        # Fetch and store product data
        logging.info("Fetching and saving product data...")
        process_keywords(keywords, output_folder)

        logging.info("Workbook saved successfully to '%s'.", output_folder)

    except Exception as e:
        logging.error("An error occurred during execution: %s", e)

    finally:
        # Log completion message with additional context
        end_time = perf_counter()
        run_time = end_time - start_time
        logging.info("===================================================")
        logging.info("APEC Auto scraping has successfully completed.")
        logging.info(f"Total runtime: {run_time:.6f} seconds")
        logging.info("Results saved to: %s", output_folder)
        logging.info("===================================================")
