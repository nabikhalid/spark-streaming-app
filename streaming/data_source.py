import sys
import requests
import socket
import time
import os
import json

TCP_IP = "0.0.0.0"
TCP_PORT = 9999
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
# if the connection is accepted, proceed
conn, addr = s.accept()
print("Connected... Starting sending data.")

#########################################################################

auth_token = os.getenv('TOKEN')

# Set up GitHub API authentication token

# Set up the search query
languages = ["python", "java", "c"]
search_url = "https://api.github.com/search/repositories"
query_params = {
    "q": " ".join(f"language:{language}" for language in languages),
    "sort": "updated",
    "order": "desc",
    "per_page": 50
}

#########################################################################

while True:
    try:
        # Send the search query to GitHub API
        response = requests.get(search_url, headers={"Authorization": f"token {auth_token}"}, params=query_params)
        response.raise_for_status()

        # Extract the relevant data from the response
        repos = []
        for item in response.json()["items"]:
            repos.append({
                "name": item["name"],
                "language": item["language"],
                "description": item["description"],
                "created_at": item["created_at"],
                "updated_at": item["updated_at"],
                "pushed_at": item["pushed_at"],
                "url": item["html_url"],
                "stargazers_count": item["stargazers_count"]
            })

        for repo in repos:
            data = f"{json.dumps(repo)}\n".encode()
            conn.send(data)
            print(repo)

        time.sleep(15)
    except KeyboardInterrupt:
        s.shutdown(socket.SHUT_RD)