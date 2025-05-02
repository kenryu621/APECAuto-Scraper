import tkinter as tk
from tkinter import messagebox

import my_libs.apec.apec_data_extraction as apec
from my_libs.logging_config import setup_logging
from my_libs.utils import get_output_directory

TXT_FILE = "Keywords.txt"
OUTPUT_FOLDER = get_output_directory(".")


def prompt_for_keywords_from_txt():
    """Create a TXT file and notify the user to enter keywords manually."""
    root = tk.Tk()
    root.withdraw()  # Hide the root window

    # Create the TXT file with a note and examples
    with open(TXT_FILE, mode="w") as file:
        file.write("# Enter your keywords here, one per line.\n")
        file.write("# Lines that start with # will be skipped.\n")
        file.write("# Example:\n")
        file.write("# 90916-03100\n")
        file.write("# 15643-31050\n")

    # Notify the user
    messagebox.showinfo(
        "TXT File Created",
        f"TXT file '{TXT_FILE}' has been created. Please open this file and enter your keywords manually."
    )


def read_keywords_from_txt():
    """Read keywords from the TXT file."""
    keywords = []

    try:
        with open(TXT_FILE, mode="r") as file:
            for line in file:
                # Strip leading and trailing whitespaces
                keyword = line.strip()
                # Skip lines that start with '#' or are empty
                if keyword and not keyword.startswith("#"):
                    keywords.append(keyword)
    except FileNotFoundError:
        # File not found, so create it
        prompt_for_keywords_from_txt()
        return []
    except Exception as e:
        raise RuntimeError(
            f"An error occurred while reading the TXT file: {e}"
        )

    return keywords


def run_apec_scraper():
    """Run the Apec scraper."""
    keywords = read_keywords_from_txt()
    if not keywords:
        messagebox.showwarning(
            "No Keywords Found",
            "No keywords found in the TXT file. Please add keywords and run the program again."
        )
        return

    apec.process_keywords(keywords, OUTPUT_FOLDER)
    messagebox.showinfo(
        "Apec Scraper",
        "Apec scraper execution completed."
    )


def main():
    setup_logging()
    run_apec_scraper()


if __name__ == "__main__":
    main()
