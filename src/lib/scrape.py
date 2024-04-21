import requests
from requests.exceptions import RequestException
from backoff import on_exception, expo
from tqdm import tqdm
import time

def fetch_with_retry(url, max_retries=3):
    @on_exception(expo, RequestException, max_tries=max_retries)
    def fetch_url_with_retry():
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    try:
        return fetch_url_with_retry(), None  # Return the URL along with the response
    except RequestException as e:
        return None, e  # Return the exception along with None response

def fetch_urls(urls):
    successes = []
    failures = []

    with tqdm(total=len(urls), desc="Fetching URLs", unit="URL") as pbar:
        start_time = time.time()  # Record the start time

        for url in urls:
            response, error = fetch_with_retry(url)

            if response is not None:
                yield response
            else:
                yield {
                    "error": error,
                    "url": url
                }
            
            pbar.update(1)  # Update the progress bar

            # Calculate time estimate based on the time taken for previous requests
            elapsed_time = time.time() - start_time
            time_per_request = elapsed_time / pbar.n
            remaining_requests = len(urls) - pbar.n
            estimated_time_remaining = time_per_request * remaining_requests
            pbar.set_postfix({"Est. Time Remaining": f"{estimated_time_remaining:.2f}s"})

    return successes, failures
