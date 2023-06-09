import avro.schema
import json
import generateAvroSchema
from deepdiff import DeepDiff
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader

def main():
    # Counter for unique test file name
    counter = 0

    # Generate avro schema
    check_file = generateAvroSchema.main(counter)

    if check_file['status'] != 200:
        return check_file['msg']
    
    # Parse schema
    schema = avro.schema.parse(open(f'test{counter}/schema{counter}_codec.avsc', 'rb').read())

    ## Serialise json file by opening the existing Avro file in append mode
    try:
        writer = DataFileWriter(open(f"test{counter}/schema{counter}_codec.avro", "wb"), DatumWriter(), schema)
        # Open the JSON file and load its contents
        with open("data.json", "r") as json_file:
            json_data = json.load(json_file)
            writer.append(json_data)
        writer.close()

        print("Serialization and AVRO file write completed successfully")
    
    except:
        raise Exception("Failed to generate avro file")
    
    ## Deserialise avro file
    schema = avro.schema.parse(open(f'test{counter}/schema{counter}_codec.avsc', 'rb').read())
    try:
        reader = DataFileReader(open(f"test{counter}/schema{counter}_codec.avro", "rb"), DatumReader(schema))

        # Write deserialized records to a JSON file
        with open(f"test{counter}/deserialized_data.json", "w") as json_file:
            for record in reader:
                # Write each record as a separate JSON object
                json.dump(record, json_file)
                json_file.write('\n')  # Add a new line after each JSON object

        reader.close()

        print("Deserialization and JSON file write completed successfully")

    except:
        raise Exception("Failed to deserialize Avro file")
    
    ## Compare with original json file
    with open(f"data.json","r") as orif:
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
