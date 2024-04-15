import json
import csv
import aiohttp
import asyncio
import requests

# Load keys from configuration file
with open('config/fmp-config-local.json', 'r') as file:
    config = json.load(file)
api_key = config["api_key"]
access_token = config["access_token"]
user_id = config["user_id"]
bot_id = config["bot_id"]
rewrite_api = config["rewrite_api"]
 
async def send_request(session, method, url, headers, json_data):
    """
    Asynchronous function that sends an HTTP request and returns the JSON response.

    Parameters:
    session (aiohttp.ClientSession): An aiohttp session object used for sending HTTP requests.
    method (str): The HTTP method for the request ('GET' or 'POST').
    url (str): The URL for the API.
    headers (dict): The headers to be used in the request.
    json_data (dict): The data to be sent as a JSON body in the request (for POST).

    Returns:
    dict: The response data as a dictionary if the request was successful, otherwise None.
    """
    # The function to call on session depends on the method
    if method.upper() == 'GET':
        func = session.get
    elif method.upper() == 'POST':
        func = session.post
    else:
        raise ValueError(f'Unsupported method: {method}')

    try:
        # Asynchronous request using aiohttp
        async with func(url, headers=headers, json=json_data) as response:
            # Return the JSON response if the request is successful
            if response.status == 200:
                return await response.json()
            else:
                return None
           
    except Exception as e:
        # Handle any exceptions that may occur
        print(f"An error occurred: {e}")  
        return None
    
def get_available_traded():
    """
    This function sends a GET request to the API and writes the response to a JSON file.
    """
    
    url = f"https://fmpcloud.io/api/v3/available-traded/list?apikey={api_key}"
    
    response = requests.get(url)
    data = response.json()
    
    with open('list.json', 'w') as file:
        json.dump(data, file)

    print("Data has been written to list.json")
        
async def get_profile_data(session, api_url, api_key, symbol):
    """
    Asynchronous function to fetch profile data for a single symbol using API.

    Parameters:
    session (aiohttp.ClientSession): An aiohttp session object used for sending HTTP requests.
    api_url (str): The base URL for the API.
    api_key (str): The API key for accessing the server.
    symbol (str): The symbol for the entity you're fetching data for.

    Returns:
    tuple: A tuple containing profile data (dict) for the fetched symbol and the symbol itself.
    """
    # Construct the url using the base api url, api key and symbol
    url = f"{api_url}{symbol}?apikey={api_key}"
    # Use the send_request function to fetch the response data from the API
    profile_data = await send_request(session, "GET", url, {}, None)
    print(profile_data)
    # Return both the profile data and the symbol it corresponds to
    return profile_data, symbol

async def update_with_profile(session, filtered_data, api_url, api_key):
    """
    Asynchronous function to update profile data for all symbols using API.

    Parameters:
    filtered_data (list): List of dictionaries containing symbol data.
    api_url (str): The base URL for the API.
    api_key (str): The API key for accessing the server.

    Returns:
    list: List of updated symbol data dictionaries with profile data.
    """
    # Create a dictionary mapping symbols to their corresponding item in filtered_data
    data_map = {item['symbol']: item for item in filtered_data}

    # Start an aiohttp ClientSession
    async with aiohttp.ClientSession() as session:
        # Create a list of tasks, where each task is fetching the profile data for a symbol
        tasks = [get_profile_data(session, api_url, api_key, symbol) for symbol in data_map.keys()]

        # Use asyncio.as_completed to process the tasks as soon as they complete
        for coro in asyncio.as_completed(tasks):
            # For each completed task, get the profile data and symbol
            profile_data, symbol = await coro

            # If profile data was successfully fetched, update the corresponding item in the data map
            if profile_data is not None:
                item = data_map[symbol]
                item.update({
                    "currency": profile_data[0]['currency'],
                    "industry": profile_data[0]['industry'],
                    "sector": profile_data[0]['sector'],
                    "country": profile_data[0]['country'],
                    "image":  profile_data[0]['image'],
                    "description":  profile_data[0]['description']
                })

    # Return the values of the data map as a list
    return list(data_map.values())
 
 # Asynchronous function to use Coze API
async def rewrite_with_coze(session, access_token, user_id, bot_id, text):
    """
    Asynchronous function that uses the Coze API to rewrite a given text.

    Parameters:
    session (aiohttp.ClientSession): An aiohttp session object used for sending HTTP requests.
    access_token (str): Access token for the Coze API.
    user_id (str): User ID for the Coze API.
    bot_id (str): Bot ID for the Coze API.
    text (str): The text to be rewritten.

    Returns:
    str: The rewritten text if the request was successful, otherwise None.
    """
    # Set headers for the request
    headers = {
        'Authorization': 'Bearer ' + access_token, 
        'Content-Type': 'application/json'
    }
    
    # URL for Coze API
    url = "https://api.coze.com/open_api/v2/chat"
       
    # JSON data to be sent with the request
    json_data = {
        'conversation_id': '123',
        'bot_id': bot_id,
        'user': user_id,
        'query': text,
        'stream': False,
    }

    # Use the send_request function to fetch the response data from the API
    res_json = await send_request(session, "POST", url, headers, json_data)

    try:
        # Check if the request was successful and return the content
        if res_json is not None:
            return res_json['messages'][0]['content']
        else:
            return None

    except Exception as e:
        # Handle exception and print the error
        print(f"An error occurred while processing the Coze API response: {e}")
        return None

# Asynchronous function to use OpenAI API
async def rewrite_with_openai(session, api_key, text):
    """
    Asynchronous function that uses the OpenAI API to rewrite a given text.

    Parameters:
    session (aiohttp.ClientSession): An aiohttp session object used for sending HTTP requests.
    api_key (str): API key for the OpenAI API.
    text (str): The text to be rewritten.

    Returns:
    str: The rewritten text if the request was successful, otherwise None.
    """
    # URL for OpenAI API
    url = "https://api.openai.com/v1/engines/davinci-codex/completions"
    
    # Define headers with API key
    headers = {
        'Authorization': 'Bearer ' + api_key, 
        'Content-Type': 'application/json'
    }
    
    # Define prompt and maximum tokens for rewritten text
    data = {
        'prompt': f"Rewrite the following text in a maximum of 50 words:\n{text}",
        'max_tokens': 50
    }

    # Use the send_request function to fetch the response data from the API
    response_data = await send_request(session, "POST", url, headers, data)

    try:
        # Return the rewritten text if response data exists
        if response_data is not None:
            return response_data['choices'][0]['text'].strip()
        else:
            return None

    except Exception as e:
        # Handle possible exceptions and print the error message
        print(f"An error occurred while processing the OpenAI API response: {e}")
        return None

# Asynchronous main function
async def main():
    # Get new data with api 
    get_available_traded()
    
    # Load and filter data from JSON file
    with open('list-test.json', 'r', encoding='utf-8') as file:
        data = json.load(file)

    filtered_data = [item for item in data if item['type'].lower() in ['stock', 'etf']
                     and item['exchangeShortName'] in ['AMEX', 'NASDAQ']]

    # Use API call to update each item in the filtered_data list
    api_url = "https://fmpcloud.io/api/v3/profile/"

    # Create a session
    async with aiohttp.ClientSession() as session:
        updated_data = await update_with_profile(session, filtered_data, api_url, api_key)

        # Iterate through each item in updated data
        for item in updated_data:
            description = item['description']
            prompt = "Can you rewrite the following text with a maximum of 50 words?"
            full_text = description + prompt

            # Decide the type of API to rewrite descriptions
            if rewrite_api.lower() == "openai":
                rewritten_description = await rewrite_with_openai(session, api_key, full_text)
            elif rewrite_api.lower() == "coze":
                rewritten_description = await rewrite_with_coze(session, access_token, user_id, bot_id, full_text)
            else:
                raise ValueError("Invalid `rewrite_api` value. It should be either 'openai' or 'coze'.")

            # Update the description for each item
            if rewritten_description is not None:
                item.update({"description-new": rewritten_description})

    # Write the updated information to a CSV file
    fieldnames = list(updated_data[0].keys())
    with open('output.csv', 'w', newline='') as csv_file:  
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in updated_data:
            writer.writerow(row)

if __name__ == "__main__":
    asyncio.run(main())