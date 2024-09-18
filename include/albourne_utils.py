import pandas as pd
import requests
from airflow.hooks.base import BaseHook

from include.global_variables import airflow_conf_variables as gv
from include.global_variables import albourne_input_variables as av


def use_http_connection(**kwargs):
    # Retrieve the connection object
    connection = BaseHook.get_connection(gv.CONN_ID_HTTP)

    # Extract the required attributes
    username = connection.login
    password = connection.password
    base_url = connection.host

    # Use these attributes in your code
    print(f"Username: {username}")
    print(f"Password: {password}")
    print(f"Base URL: {base_url}")

    conn_info = {"username": username, "password": password, "base_url": base_url}
    # Return these attributes as a list
    return conn_info


def get_bearer_token(username, password):
    request_url = f"{av.BASE_URL}/auth/login"
    credentials = {"username": username, "password": password}

    # Add the headers required by the API
    headers = {
        "Accept": "*/*",
        "Content-Type": "application/json"
    }

    response = requests.post(request_url, json=credentials, headers=headers)

    if response.status_code == 200:
        token = response.json().get("token")
        if token:
            bearer_token = f"Bearer {token}"
            return bearer_token
        else:
            raise ValueError("Token not found in the response headers")
    else:
        raise ValueError(f"Request failed with status code: {response.status_code}")


bearer_token = get_bearer_token(av.USERNAME, av.PASSWORD)


# print(bearer_token)


def get_all_funds(token):
    request_url = f"{av.BASE_URL}/data/rest/allFunds"

    # Add the headers required by the API
    headers = {
        "Accept": "*/*",
        "Content-Type": "application/json",
        "Authorization": token
    }

    response = requests.get(request_url, headers=headers)

    if response.status_code == 200:
        all_funds_df = pd.DataFrame(response.json())
        if not all_funds_df.empty:
            return all_funds_df
        else:
            raise ValueError("No data found in the response")
    else:
        raise ValueError(f"Request failed with status code: {response.status_code}")

# all_funds = get_all_funds(bearer_token)
# print(all_funds)
