import requests

def main():
    url = "http://127.0.0.1:8000/"
    body = {
        "shop_info": {
            "name": "B shop",
            "location": "Shizuoka"
        },
        "items": [
            {
            "name": "T-shirt",
            "description": "This in normal T-shirt",
            "price": "500",
            "tax": "1.1"
            }
        ]
    }
    
    res = requests.post(url, json = body)
    print(res.json())


if __name__ == "__main__":
    main()