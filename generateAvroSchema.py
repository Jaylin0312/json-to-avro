import requests
import json
import os.path


def main(counter: int):
    # The website that converts json file to avro schema
    url = "http://www.dataedu.ca/avro/api/v1/json-doc-to-avro"

    # Define payload
    with open("data.json") as f:
        payload = json.load(f)

    # Post request to url
    try:
        response = requests.post(url, json=payload)
    except requests.exceptions.RequestException as e:
        raise Exception("Post request failed: " + str(e))

    # Retrieve returned avro schema and write into an avro file
    try:
        if not os.path.exists(f"test{counter}"):
            os.makedirs(f"test{counter}")

        with open(f"test{counter}/schema{counter}_codec.avsc", "w") as f:
            json.dump(response.json(), f)
        return {"status": 200, "msg": "avro schema file successfully created"}
    except:
        return {"status": 404, "msg": "avro schema file creation failed"}


if __name__ == "__main__":
    main()
