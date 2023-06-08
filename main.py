import avro.schema
import json
import generateAvroSchema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter


def main():
    # Counter for dynamic file name
    counter = 0

    # Generate avro schema
    check_file = generateAvroSchema.main(counter)

    if check_file["status"] != 200:
        return check_file["msg"]

    # Parse schema
    schema = avro.schema.parse(
        open(f"test{counter}/schema{counter}_codec.avsc", "rb").read()
    )

    try:
        writer = DataFileWriter(
            open(f"test{counter}/schema{counter}_codec.avro", "wb"),
            DatumWriter(),
            schema,
        )
        # Open the JSON file and load its contents
        with open("data.json", "r") as json_file:
            json_data = json.load(json_file)
            writer.append(json_data)
        writer.close()

        return "Avro file generated"

    except:
        raise Exception("Failed to generate avro file")


if __name__ == "__main__":
    main()
