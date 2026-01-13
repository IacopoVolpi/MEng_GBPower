from requests.exceptions import HTTPError
import time

#this function makes robust HTTP requests with retries and error handling. it is useful for ensuring reliable data retrieval from web services that may experience intermittent issues.
def robust_request(getfunc, *args, max_retries=100, wait_time=5, **kwargs):

    for _ in range(max_retries):
        try:
            #this calls the provided function with its arguments to make the HTTP request
            #if the code inside try block runs without exceptions, it returns the response else it goes to except block without crashing.
            response = getfunc(*args, **kwargs)
            return response
        #this handles HTTP errors specifically, logging the error message
        except HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except Exception as err:
            print(f"Other error occurred: {err}")
            #wait before retrying
        time.sleep(wait_time)

    raise Exception(f"Failed to get response after {max_retries} retries")