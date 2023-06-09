# json-to-avro

This is a simple script that converts json file to avro schema and file.

## Steps to execute
1. Create a `data.json` file in the root directory
2. Put in the json data that you would like to convert to avro file
3. Run `python3 main.py`. Make sure the counter tallies with the number you want to use (eg. index of current test case)
4. At the end of the process, you should see three files in the test folder
- deserialised_data.json
- schema_codec.avro
- schema_codec.avsc
5. You should see `"Process completed successfully"` if the avro file generated is the same as the original json file after deserialization

## Important notes
Credit Website: http://www.dataedu.ca/avro
This project posts a request to external website to generate avro schema from the json file. Do verify the schema and change accordingly depends on your use case to ensure the generated avro file is correct.