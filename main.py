import avro.schema
import json
import generateAvroSchema
from deepdiff import DeepDiff
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader
from avro import datafile, io


def main():
    # Counter for unique test file name
    counter = 4

    # Generate avro schema
    check_file = generateAvroSchema.main(counter)

    if check_file["status"] != 200:
        return check_file["msg"]

    # Parse schema
    schema = avro.schema.parse(
        open(f"test{counter}/schema{counter}_codec.avsc", "rb").read()
    )

    ## Serialise json file by opening the existing Avro file in append mode
    try:
        # Configure the Avro writer with Snappy or Zstandard codec
        avro_writer = datafile.DataFileWriter(
            open(f"test{counter}/schema{counter}_codec.avro", "wb"),
            io.DatumWriter(),
            schema,
            codec="snappy",
        )

        # Open the JSON file and load its contents
        with open("data.json", "r") as json_file:
            json_data = json.load(json_file)
            avro_writer.append(json_data)
        avro_writer.close()

        print("Serialization and AVRO file write completed successfully")

    except:
        raise Exception("Failed to generate avro file")

    ## Deserialise avro file
    schema = avro.schema.parse(
        open(f"test{counter}/schema{counter}_codec.avsc", "rb").read()
    )
    try:
        # Configure the Avro reader with Snappy or Zstandard codec
        avro_reader = datafile.DataFileReader(
            open(f"test{counter}/schema{counter}_codec.avro", "rb"),
            io.DatumReader(schema),
            codec="snappy",  # Change the codec here to 'snappy' or 'zstandard'
        )

        # Write deserialized records to a JSON file
        with open(f"test{counter}/deserialized_data.json", "w") as json_file:
            for record in avro_reader:
                # Write each record as a separate JSON object
                json.dump(record, json_file)
                json_file.write("\n")  # Add a new line after each JSON object

        avro_reader.close()

        print("Deserialization and JSON file write completed successfully")

    except:
        raise Exception("Failed to deserialize Avro file")

    ## Compare with original json file
    with open(f"test{counter}/data.json", "r") as orif:
        original_data = json.load(orif)
        orif.close()

    with open(f"test{counter}/deserialized_data.json", "r") as newf:
        deserialized_data = json.load(newf)
        newf.close()

    result = DeepDiff(original_data, deserialized_data)

    if result == {}:
        print("Process completed successfully")
    else:
        raise Exception(result)


if __name__ == "__main__":
    main()
