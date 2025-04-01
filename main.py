from pathlib import Path
import re
from urllib.parse import urlparse

def is_valid_url(url):
    try:
        parsed = urlparse(url)
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)
    except Exception:
        return False

def is_valid_domain(domain):
    domain_regex = re.compile(r"^(?!-)(?:[a-zA-Z0-9-]{1,63}\.)+[a-zA-Z]{2,}$")
    return bool(domain_regex.fullmatch(domain))

def process_strings(input_list, inventory_type, http_client):
    for input_str in input_list:
        if is_valid_domain_url(input_str):
            # Valid URL or domain — continue with the rest of your loop logic here
            # For example:
            print(f"Processing: {input_str}")
            # ... your logic here ...
            continue
        
        # Invalid string — log failure and skip
        error_msg = "Invalid input: not a URL or domain"
        repr_error_msg = repr(error_msg)
        response_code = 400
        headers_json = "{}"

        print("log error")

        continue  # Go to next string

def is_valid_domain_url(input_str):
    if input_str is None:
        return False
    return is_valid_url(input_str) or is_valid_domain(input_str)

# input_list = ["https://example.com", "invalid@site", "valid-domain.org", None, "", " "]
# process_strings(input_list, inventory_type="display", http_client="requests")


# path_testing = Path("scrapy_logs") / f"scrapy_log_{"file_suffix"}.log"
# print(path_testing)
# print(path_testing.parent)


test_list_tup = [(11,12),(13,14)]

for i, j in test_list_tup:
    print(i)
    print(j)
